package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNotificationLifecycle(t *testing.T) {
	type receivedRequest struct {
		method string
		header http.Header
		body   string
	}

	var flakyCalls atomic.Int32
	var receivedMu sync.Mutex
	var received []receivedRequest
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read vendor request: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		receivedMu.Lock()
		received = append(received, receivedRequest{r.Method, r.Header.Clone(), string(body)})
		receivedMu.Unlock()

		switch r.URL.Path {
		case "/ok":
			w.WriteHeader(http.StatusNoContent)
		case "/flaky":
			if flakyCalls.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case "/bad":
			w.WriteHeader(http.StatusBadRequest)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(vendor.Close)

	cfg := defaultConfig()
	cfg.DBPath = filepath.Join(t.TempDir(), "notifyd.db")
	cfg.AllowPrivateNetworks = true
	cfg.WorkerCount = 3
	cfg.PerHostConcurrency = 2
	cfg.MaxAttempts = 3
	cfg.PollInterval = 5 * time.Millisecond
	cfg.BaseBackoff = 10 * time.Millisecond
	cfg.MaxBackoff = 20 * time.Millisecond
	cfg.LeaseDuration = 200 * time.Millisecond
	cfg.RequestTimeout = time.Second

	service, err := newService(cfg)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	service.start()
	t.Cleanup(service.close)

	api := httptest.NewServer(service.handler())
	t.Cleanup(api.Close)

	t.Run("successful delivery", func(t *testing.T) {
		result := submitNotification(t, api.URL, "success-1", vendor.URL+"/ok", `{"contact_id":"c-1"}`)
		if result.code != http.StatusAccepted {
			t.Fatalf("enqueue status = %d, want 202; body=%s", result.code, result.body)
		}

		view := waitForStatus(t, api.URL, result.id, "succeeded")
		if view.AttemptCount != 1 || len(view.Attempts) != 1 || view.Attempts[0].Outcome != "succeeded" {
			t.Fatalf("unexpected attempts: %+v", view)
		}

		receivedMu.Lock()
		request := received[0]
		receivedMu.Unlock()
		if request.method != http.MethodPost || request.header.Get("X-Vendor-Token") != "secret" || request.body != `{"contact_id":"c-1"}` {
			t.Fatalf("unexpected vendor request: %+v", request)
		}
	})

	t.Run("5xx is retried", func(t *testing.T) {
		result := submitNotification(t, api.URL, "retry-1", vendor.URL+"/flaky", `{"contact_id":"c-2"}`)
		view := waitForStatus(t, api.URL, result.id, "succeeded")
		if view.AttemptCount != 2 || len(view.Attempts) != 2 {
			t.Fatalf("attempt count = %d (%d records), want 2", view.AttemptCount, len(view.Attempts))
		}
		if view.Attempts[0].Outcome != "retryable_status" || view.Attempts[1].Outcome != "succeeded" {
			t.Fatalf("unexpected retry outcomes: %+v", view.Attempts)
		}
	})

	t.Run("other 4xx is dead-lettered", func(t *testing.T) {
		result := submitNotification(t, api.URL, "dead-1", vendor.URL+"/bad", `{"contact_id":"c-3"}`)
		view := waitForStatus(t, api.URL, result.id, "dead")
		if view.AttemptCount != 1 || view.LastStatus == nil || *view.LastStatus != http.StatusBadRequest {
			t.Fatalf("unexpected dead-letter state: %+v", view)
		}
	})

	t.Run("idempotency key deduplicates only the same request", func(t *testing.T) {
		first := submitNotification(t, api.URL, "same-key", vendor.URL+"/ok", `{"contact_id":"c-4"}`)
		second := submitNotification(t, api.URL, "same-key", vendor.URL+"/ok", `{"contact_id":"c-4"}`)
		if first.code != http.StatusAccepted || second.code != http.StatusAccepted || first.id != second.id {
			t.Fatalf("same request was not deduplicated: first=%+v second=%+v", first, second)
		}
		waitForStatus(t, api.URL, first.id, "succeeded")
		replay := submitNotification(t, api.URL, "same-key", vendor.URL+"/ok", `{"contact_id":"c-4"}`)
		if replay.code != http.StatusAccepted || replay.id != first.id || replay.status != "succeeded" {
			t.Fatalf("duplicate after success = %+v, want succeeded %s", replay, first.id)
		}

		conflict := submitNotification(t, api.URL, "same-key", vendor.URL+"/ok", `{"contact_id":"changed"}`)
		if conflict.code != http.StatusConflict {
			t.Fatalf("changed request status = %d, want 409; body=%s", conflict.code, conflict.body)
		}
	})

	t.Run("private targets are blocked by default", func(t *testing.T) {
		blockedCfg := defaultConfig()
		blockedCfg.DBPath = filepath.Join(t.TempDir(), "blocked.db")
		blockedService, err := newService(blockedCfg)
		if err != nil {
			t.Fatalf("new blocked service: %v", err)
		}
		t.Cleanup(blockedService.close)
		blockedAPI := httptest.NewServer(blockedService.handler())
		t.Cleanup(blockedAPI.Close)

		result := submitNotification(t, blockedAPI.URL, "blocked-1", vendor.URL+"/ok", `{}`)
		if result.code != http.StatusBadRequest || result.errorCode != "target_blocked" {
			t.Fatalf("private target response = %+v, want 400 target_blocked", result)
		}
	})
}

type submitResult struct {
	code      int
	id        string
	status    string
	body      string
	errorCode string
}

func submitNotification(t *testing.T, apiURL, key, targetURL, body string) submitResult {
	t.Helper()
	payload := fmt.Sprintf(`{"url":%q,"method":"POST","headers":{"Content-Type":"application/json","X-Vendor-Token":"secret"},"body":%s}`, targetURL, body)
	req, err := http.NewRequest(http.MethodPost, apiURL+"/v1/notifications", bytes.NewBufferString(payload))
	if err != nil {
		t.Fatalf("create enqueue request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("enqueue request: %v", err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read enqueue response: %v", err)
	}
	var decoded struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Error  struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		t.Fatalf("decode enqueue response %q: %v", responseBody, err)
	}
	return submitResult{resp.StatusCode, decoded.ID, decoded.Status, string(responseBody), decoded.Error.Code}
}

type notificationView struct {
	Status       string `json:"status"`
	AttemptCount int    `json:"attempt_count"`
	LastStatus   *int   `json:"last_status"`
	Attempts     []struct {
		Outcome string `json:"outcome"`
	} `json:"attempts"`
}

func waitForStatus(t *testing.T, apiURL, id, wanted string) notificationView {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(apiURL + "/v1/notifications/" + id)
		if err != nil {
			t.Fatalf("get notification: %v", err)
		}
		var view notificationView
		err = json.NewDecoder(resp.Body).Decode(&view)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("decode notification: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("get notification status = %d", resp.StatusCode)
		}
		if view.Status == wanted {
			return view
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("notification %s did not reach %s", id, wanted)
	return notificationView{}
}
