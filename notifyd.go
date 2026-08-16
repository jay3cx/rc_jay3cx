package main

import (
	cryptorand "crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type service struct {
	cfg config
	db  *sql.DB
}

func newService(cfg config) (*service, error) {
	if cfg.DBPath == "" {
		return nil, errors.New("invalid service configuration")
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
	} {
		if _, err := db.Exec(statement); err != nil {
			db.Close()
			return nil, err
		}
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS notifications (
		id TEXT PRIMARY KEY,
		method TEXT NOT NULL,
		target_url TEXT NOT NULL,
		headers BLOB NOT NULL,
		body BLOB NOT NULL,
		status TEXT NOT NULL CHECK (status = 'pending'),
		created_at INTEGER NOT NULL,
		updated_at INTEGER NOT NULL
	)`)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &service{cfg: cfg, db: db}, nil
}

func (s *service) close() {
	_ = s.db.Close()
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
		INSERT INTO notifications (id, method, target_url, headers, body, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)`,
		id, method, target.String(), headerBytes, []byte(input.Body), now, now)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", "notification could not be stored")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"id": id, "status": "pending"})
}

type notificationResponse struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

func (s *service) getNotification(w http.ResponseWriter, r *http.Request) {
	var response notificationResponse
	var createdAt, updatedAt int64
	err := s.db.QueryRowContext(r.Context(), `
		SELECT id, status, created_at, updated_at FROM notifications WHERE id = ?`,
		r.PathValue("id"),
	).Scan(&response.ID, &response.Status, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "notification was not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "storage_error", "notification could not be read")
		return
	}
	response.CreatedAt = formatTime(createdAt)
	response.UpdatedAt = formatTime(updatedAt)
	writeJSON(w, http.StatusOK, response)
}

func (s *service) health(w http.ResponseWriter, r *http.Request) {
	if err := s.db.PingContext(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "unhealthy", "database is unavailable")
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok\n")
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
