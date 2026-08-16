package main

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type service struct {
	cfg       config
	db        *sql.DB
	client    *http.Client
	ctx       context.Context
	cancel    context.CancelFunc
	workers   sync.WaitGroup
	startOnce sync.Once
	closeOnce sync.Once
}

func newService(cfg config) (*service, error) {
	if cfg.DBPath == "" || cfg.WorkerCount < 1 {
		return nil, errors.New("invalid service configuration")
	}
	if cfg.PollInterval <= 0 || cfg.LeaseDuration <= 0 || cfg.RequestTimeout <= 0 {
		return nil, errors.New("invalid service timing configuration")
	}

	db, err := sql.Open("sqlite", cfg.DBPath)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, statement := range []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA foreign_keys = ON",
	} {
		if _, err := db.Exec(statement); err != nil {
			db.Close()
			return nil, err
		}
	}
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS notifications (
			id TEXT PRIMARY KEY,
			method TEXT NOT NULL,
			target_url TEXT NOT NULL,
			headers BLOB NOT NULL,
			body BLOB NOT NULL,
			status TEXT NOT NULL CHECK (status IN ('pending', 'delivering', 'succeeded', 'dead')),
			attempt_count INTEGER NOT NULL DEFAULT 0,
			next_attempt_at INTEGER NOT NULL,
			lease_until INTEGER,
			lease_token TEXT,
			last_status INTEGER,
			last_error TEXT,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS notifications_due
			ON notifications(status, next_attempt_at, lease_until)`,
		`CREATE TABLE IF NOT EXISTS delivery_attempts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			notification_id TEXT NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
			number INTEGER NOT NULL,
			started_at INTEGER NOT NULL,
			finished_at INTEGER,
			outcome TEXT NOT NULL,
			status_code INTEGER
		)`,
		`CREATE INDEX IF NOT EXISTS attempts_by_notification
			ON delivery_attempts(notification_id, id)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			db.Close()
			return nil, err
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &service{
		cfg:    cfg,
		db:     db,
		client: &http.Client{Timeout: cfg.RequestTimeout},
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func (s *service) start() {
	s.startOnce.Do(func() {
		for i := 0; i < s.cfg.WorkerCount; i++ {
			s.workers.Add(1)
			go s.work()
		}
	})
}

func (s *service) close() {
	s.closeOnce.Do(func() {
		s.cancel()
		s.workers.Wait()
		if err := s.db.Close(); err != nil {
			log.Printf("close database: %v", err)
		}
	})
}

func (s *service) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/notifications", s.createNotification)
	mux.HandleFunc("GET /v1/notifications/{id}", s.getNotification)
	mux.HandleFunc("GET /healthz", s.health)
	return mux
}

type notificationRequest struct {
	URL     string            `json:"url"`
	Method  string            `json:"method,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    json.RawMessage   `json:"body,omitempty"`
}

func (s *service) createNotification(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var input notificationRequest
	if err := decoder.Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must be valid JSON")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid_request", "request body must contain one JSON object")
		return
	}

	method := strings.ToUpper(strings.TrimSpace(input.Method))
	if method == "" {
		method = http.MethodPost
	}
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
	default:
		writeError(w, http.StatusBadRequest, "invalid_method", "method must be POST, PUT, PATCH, or DELETE")
		return
	}
	target, err := parseTarget(input.URL)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_target", "url must be an absolute HTTP(S) URL")
		return
	}
	headerBytes, err := json.Marshal(input.Headers)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_headers", "headers could not be encoded")
		return
	}
	if input.Body == nil {
		input.Body = json.RawMessage{}
	}
	id, err := randomID()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "notification id could not be generated")
		return
	}
	now := time.Now().UTC().UnixMilli()
	_, err = s.db.ExecContext(r.Context(), `
		INSERT INTO notifications (
			id, method, target_url, headers, body, status, next_attempt_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)`,
		id, method, target.String(), headerBytes, []byte(input.Body), now, now, now)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", "notification could not be stored")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"id": id, "status": "pending"})
}

type notificationResponse struct {
	ID           string            `json:"id"`
	Status       string            `json:"status"`
	AttemptCount int               `json:"attempt_count"`
	LastStatus   *int              `json:"last_status,omitempty"`
	LastError    string            `json:"last_error,omitempty"`
	CreatedAt    string            `json:"created_at"`
	UpdatedAt    string            `json:"updated_at"`
	Attempts     []attemptResponse `json:"attempts"`
}

type attemptResponse struct {
	Number     int     `json:"number"`
	StartedAt  string  `json:"started_at"`
	FinishedAt *string `json:"finished_at,omitempty"`
	Outcome    string  `json:"outcome"`
	StatusCode *int    `json:"status_code,omitempty"`
}

func (s *service) getNotification(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var response notificationResponse
	var lastStatus sql.NullInt64
	var lastError sql.NullString
	var createdAt, updatedAt int64
	err := s.db.QueryRowContext(r.Context(), `
		SELECT id, status, attempt_count, last_status, last_error, created_at, updated_at
		FROM notifications WHERE id = ?`, id,
	).Scan(&response.ID, &response.Status, &response.AttemptCount, &lastStatus, &lastError, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "notification was not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", "notification could not be read")
		return
	}
	if lastStatus.Valid {
		value := int(lastStatus.Int64)
		response.LastStatus = &value
	}
	if lastError.Valid {
		response.LastError = lastError.String
	}
	response.CreatedAt = formatTime(createdAt)
	response.UpdatedAt = formatTime(updatedAt)
	response.Attempts = make([]attemptResponse, 0)

	rows, err := s.db.QueryContext(r.Context(), `
		SELECT number, started_at, finished_at, outcome, status_code
		FROM delivery_attempts WHERE notification_id = ? ORDER BY id`, id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", "delivery attempts could not be read")
		return
	}
	defer rows.Close()
	for rows.Next() {
		var attempt attemptResponse
		var startedAt int64
		var finishedAt, statusCode sql.NullInt64
		if err := rows.Scan(&attempt.Number, &startedAt, &finishedAt, &attempt.Outcome, &statusCode); err != nil {
			writeError(w, http.StatusInternalServerError, "storage_error", "delivery attempts could not be read")
			return
		}
		attempt.StartedAt = formatTime(startedAt)
		if finishedAt.Valid {
			value := formatTime(finishedAt.Int64)
			attempt.FinishedAt = &value
		}
		if statusCode.Valid {
			value := int(statusCode.Int64)
			attempt.StatusCode = &value
		}
		response.Attempts = append(response.Attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", "delivery attempts could not be read")
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *service) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.db.PingContext(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "unhealthy", "database is unavailable")
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok\n")
}

func (s *service) work() {
	defer s.workers.Done()
	for {
		task, err := s.claim(s.ctx)
		if err == nil {
			s.process(task)
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) && !errors.Is(err, context.Canceled) {
			log.Printf("claim notification: %v", err)
		}
		timer := time.NewTimer(s.cfg.PollInterval)
		select {
		case <-s.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

type deliveryTask struct {
	id         string
	leaseToken string
	method     string
	targetURL  string
	headers    []byte
	body       []byte
}

func (s *service) claim(ctx context.Context) (deliveryTask, error) {
	leaseToken, err := randomID()
	if err != nil {
		return deliveryTask{}, err
	}
	now := time.Now().UTC()
	leaseUntil := now.Add(s.cfg.LeaseDuration).UnixMilli()
	var task deliveryTask
	err = s.db.QueryRowContext(ctx, `
		UPDATE notifications
		SET status = 'delivering', lease_until = ?, lease_token = ?, updated_at = ?
		WHERE id = (
			SELECT id FROM notifications
			WHERE (status = 'pending' AND next_attempt_at <= ?)
				OR (status = 'delivering' AND lease_until <= ?)
			ORDER BY CASE WHEN status = 'delivering' THEN lease_until ELSE next_attempt_at END, created_at
			LIMIT 1
		)
		RETURNING id, method, target_url, headers, body`,
		leaseUntil, leaseToken, now.UnixMilli(), now.UnixMilli(), now.UnixMilli(),
	).Scan(&task.id, &task.method, &task.targetURL, &task.headers, &task.body)
	task.leaseToken = leaseToken
	return task, err
}

func (s *service) process(task deliveryTask) {
	attemptID, attemptNumber, err := s.beginAttempt(task)
	if err != nil {
		if !errors.Is(err, errLeaseLost) && !errors.Is(err, context.Canceled) {
			log.Printf("begin delivery notification_id=%s: %v", task.id, err)
		}
		return
	}
	result := s.send(task)
	if result.aborted {
		return
	}
	state, updated, err := s.finishAttempt(task, attemptID, result)
	if err != nil {
		log.Printf("finish delivery notification_id=%s: %v", task.id, err)
		return
	}
	if updated {
		log.Printf("notification_id=%s target=%s attempt=%d outcome=%s state=%s", task.id, task.targetURL, attemptNumber, result.outcome, state)
	}
}

var errLeaseLost = errors.New("notification lease was lost")

func (s *service) beginAttempt(task deliveryTask) (int64, int, error) {
	tx, err := s.db.BeginTx(s.ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()
	var attemptNumber int
	err = tx.QueryRowContext(s.ctx, `
		UPDATE notifications
		SET attempt_count = attempt_count + 1, updated_at = ?
		WHERE id = ? AND status = 'delivering' AND lease_token = ?
		RETURNING attempt_count`,
		time.Now().UTC().UnixMilli(), task.id, task.leaseToken,
	).Scan(&attemptNumber)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, errLeaseLost
	}
	if err != nil {
		return 0, 0, err
	}
	result, err := tx.ExecContext(s.ctx, `
		INSERT INTO delivery_attempts (notification_id, number, started_at, outcome)
		VALUES (?, ?, ?, 'in_progress')`, task.id, attemptNumber, time.Now().UTC().UnixMilli())
	if err != nil {
		return 0, 0, err
	}
	attemptID, err := result.LastInsertId()
	if err != nil {
		return 0, 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	return attemptID, attemptNumber, nil
}

type deliveryResult struct {
	outcome    string
	statusCode *int
	succeeded  bool
	aborted    bool
}

func (s *service) send(task deliveryTask) deliveryResult {
	if s.ctx.Err() != nil {
		return deliveryResult{aborted: true}
	}
	var headers map[string]string
	if err := json.Unmarshal(task.headers, &headers); err != nil {
		return deliveryResult{outcome: "invalid_request"}
	}
	request, err := http.NewRequestWithContext(s.ctx, task.method, task.targetURL, bytes.NewReader(task.body))
	if err != nil {
		return deliveryResult{outcome: "invalid_request"}
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := s.client.Do(request)
	if err != nil {
		if s.ctx.Err() != nil {
			return deliveryResult{aborted: true}
		}
		return deliveryResult{outcome: "network_error"}
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 32<<10))
	response.Body.Close()
	statusCode := response.StatusCode
	if statusCode >= 200 && statusCode < 300 {
		return deliveryResult{outcome: "succeeded", statusCode: &statusCode, succeeded: true}
	}
	return deliveryResult{outcome: "http_error", statusCode: &statusCode}
}

func (s *service) finishAttempt(task deliveryTask, attemptID int64, delivery deliveryResult) (string, bool, error) {
	now := time.Now().UTC()
	state := "dead"
	var lastError any = delivery.outcome
	if delivery.succeeded {
		state = "succeeded"
		lastError = nil
	}
	var statusCode any
	if delivery.statusCode != nil {
		statusCode = *delivery.statusCode
	}

	tx, err := s.db.BeginTx(s.ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(s.ctx, `
		UPDATE delivery_attempts
		SET finished_at = ?, outcome = ?, status_code = ?
		WHERE id = ?`, now.UnixMilli(), delivery.outcome, statusCode, attemptID); err != nil {
		return "", false, err
	}
	result, err := tx.ExecContext(s.ctx, `
		UPDATE notifications
		SET status = ?, lease_until = NULL, lease_token = NULL,
			last_status = ?, last_error = ?, updated_at = ?
		WHERE id = ? AND status = 'delivering' AND lease_token = ?`,
		state, statusCode, lastError, now.UnixMilli(), task.id, task.leaseToken)
	if err != nil {
		return "", false, err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return "", false, err
	}
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	return state, updated == 1, nil
}

func parseTarget(raw string) (*url.URL, error) {
	target, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || target.Hostname() == "" || target.User != nil || target.Fragment != "" || target.Opaque != "" {
		return nil, errors.New("invalid target")
	}
	target.Scheme = strings.ToLower(target.Scheme)
	if target.Scheme != "http" && target.Scheme != "https" {
		return nil, errors.New("invalid target")
	}
	return target, nil
}

func randomID() (string, error) {
	value := make([]byte, 16)
	if _, err := cryptorand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func formatTime(milliseconds int64) string {
	return time.UnixMilli(milliseconds).UTC().Format(time.RFC3339Nano)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{"code": code, "message": message},
	})
}
