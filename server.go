package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/qmrkt/resolution-engine/dag"
)

// validRunIDPattern keeps user-supplied run_ids filename-safe; the durable
// store uses them as path components (runs/<run_id>.json).
var validRunIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

func isValidRunID(runID string) bool {
	return validRunIDPattern.MatchString(runID)
}

type RunManagerAPI interface {
	Submit(RunRequest) (RunResult, error)
	Get(string) (RunResult, bool)
	Cancel(string) (RunResult, bool)
	ActiveCount() int
}

type SignalRunManagerAPI interface {
	Signal(signalRequest) (signalResult, error)
}

type EngineServer struct {
	manager      RunManagerAPI
	startedAt    time.Time
	token        string
	shuttingDown atomic.Bool
}

type runHTTPRequest struct {
	AppID         int               `json:"app_id"`
	BlueprintJSON json.RawMessage   `json:"blueprint_json"`
	Inputs        map[string]string `json:"inputs"`
	BlueprintPath string            `json:"blueprint_path"`
	Initiator     string            `json:"initiator"`
	CallbackURL   string            `json:"callback_url,omitempty"`
}

func NewEngineServer(manager RunManagerAPI, token string) *EngineServer {
	return &EngineServer{
		manager:   manager,
		startedAt: time.Now(),
		token:     strings.TrimSpace(token),
	}
}

// BeginShutdown marks the server as draining so new mutating requests are
// rejected while read-only requests can still complete.
func (s *EngineServer) BeginShutdown() {
	if s == nil {
		return
	}
	s.shuttingDown.Store(true)
}

func (s *EngineServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/run", s.handleRun)
	mux.HandleFunc("/runs/", s.handleRunByID)
	mux.HandleFunc("/signals", s.handleSignal)
	mux.HandleFunc("/health", s.handleHealth)
	return mux
}

func (s *EngineServer) handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.authorize(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if s.rejectMutatingWhileShuttingDown(w) {
		return
	}

	var payload runHTTPRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	if payload.AppID <= 0 || len(payload.BlueprintJSON) == 0 {
		http.Error(w, "app_id and blueprint_json are required", http.StatusBadRequest)
		return
	}
	var blueprint dag.Blueprint
	if err := json.Unmarshal(payload.BlueprintJSON, &blueprint); err != nil {
		http.Error(w, "blueprint_json must be a valid blueprint object", http.StatusBadRequest)
		return
	}
	validation := ValidateResolutionBlueprint(blueprint, payload.BlueprintJSON)
	if !validation.Valid {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(validation)
		return
	}
	if payload.Inputs == nil {
		payload.Inputs = map[string]string{}
	}

	result, err := s.manager.Submit(RunRequest{
		RunID:         uuid.New().String(),
		AppID:         payload.AppID,
		BlueprintJSON: append([]byte(nil), payload.BlueprintJSON...),
		Inputs:        payload.Inputs,
		BlueprintPath: strings.TrimSpace(payload.BlueprintPath),
		Initiator:     strings.TrimSpace(payload.Initiator),
		CallbackURL:   strings.TrimSpace(payload.CallbackURL),
	})
	if err != nil {
		var dup *duplicateRunError
		if errors.As(err, &dup) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error":  err.Error(),
				"run_id": dup.RunID,
			})
			return
		}
		var full *queueFullError
		if errors.As(err, &full) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": err.Error(),
			})
			return
		}
		if errors.Is(err, errDurableManagerShuttingDown) {
			http.Error(w, "server shutting down", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(result)
}

func (s *EngineServer) handleSignal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.authorize(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if s.rejectMutatingWhileShuttingDown(w) {
		return
	}
	manager, ok := s.manager.(SignalRunManagerAPI)
	if !ok {
		http.Error(w, "signals are not supported by this run manager", http.StatusNotImplemented)
		return
	}

	var payload signalRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	if trimmed := strings.TrimSpace(payload.RunID); trimmed != "" && !isValidRunID(trimmed) {
		http.Error(w, "invalid run_id", http.StatusBadRequest)
		return
	}
	result, err := manager.Signal(payload)
	if err != nil {
		if errors.Is(err, errDurableManagerShuttingDown) {
			http.Error(w, "server shutting down", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func (s *EngineServer) handleRunByID(w http.ResponseWriter, r *http.Request) {
	if !s.authorize(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	runID := strings.TrimPrefix(r.URL.Path, "/runs/")
	if strings.TrimSpace(runID) == "" {
		http.Error(w, "run_id is required", http.StatusBadRequest)
		return
	}
	if !isValidRunID(runID) {
		http.Error(w, "invalid run_id", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		result, ok := s.manager.Get(runID)
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	case http.MethodDelete:
		if s.rejectMutatingWhileShuttingDown(w) {
			return
		}
		result, ok := s.manager.Cancel(runID)
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *EngineServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":         "ok",
		"uptime_seconds": int(time.Since(s.startedAt).Seconds()),
		"active_runs":    s.manager.ActiveCount(),
	})
}

func (s *EngineServer) authorize(r *http.Request) bool {
	if s == nil || s.token == "" {
		return true
	}
	return r.Header.Get("Authorization") == "Bearer "+s.token
}

func (s *EngineServer) rejectMutatingWhileShuttingDown(w http.ResponseWriter) bool {
	if s == nil || !s.shuttingDown.Load() {
		return false
	}
	http.Error(w, "server shutting down", http.StatusServiceUnavailable)
	return true
}
