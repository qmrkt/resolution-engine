package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

type stubRunManager struct {
	submitFn func(RunRequest) (RunResult, error)
	getFn    func(string) (RunResult, bool)
	cancelFn func(string) (RunResult, bool)
	signalFn func(signalRequest) (signalResult, error)
}

func (s *stubRunManager) Submit(req RunRequest) (RunResult, error) {
	if s.submitFn == nil {
		return RunResult{}, errors.New("submit not implemented")
	}
	return s.submitFn(req)
}
func (s *stubRunManager) Get(runID string) (RunResult, bool) {
	if s.getFn == nil {
		return RunResult{}, false
	}
	return s.getFn(runID)
}
func (s *stubRunManager) Cancel(runID string) (RunResult, bool) {
	if s.cancelFn == nil {
		return RunResult{}, false
	}
	return s.cancelFn(runID)
}
func (s *stubRunManager) Signal(req signalRequest) (signalResult, error) {
	if s.signalFn == nil {
		return signalResult{}, errors.New("signal not implemented")
	}
	return s.signalFn(req)
}
func (s *stubRunManager) ActiveCount() int { return 2 }

func TestServerPostRunReturnsAccepted(t *testing.T) {
	server := NewEngineServer(&stubRunManager{submitFn: func(req RunRequest) (RunResult, error) {
		if req.AppID != 21 {
			t.Fatalf("app_id = %d, want 21", req.AppID)
		}
		if len(req.BlueprintJSON) == 0 {
			t.Fatal("expected blueprint_json to be populated")
		}
		return RunResult{RunID: req.RunID, AppID: 21, Status: RunStatusAccepted}, nil
	}}, "")

	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader([]byte(`{"app_id":21,"blueprint_json":{"id":"bp","version":1,"nodes":[{"id":"judge","type":"human_judge","config":{"prompt":"Resolve this.","allowed_responders":["creator"],"timeout_seconds":3600}},{"id":"submit","type":"submit_result","config":{"outcome_key":"judge.outcome"}}],"edges":[{"from":"judge","to":"submit"}]}}`)))
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusAccepted)
	}
}

func TestServerPostRunRejectsInvalidRequest(t *testing.T) {
	server := NewEngineServer(&stubRunManager{}, "")

	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader([]byte(`{"app_id":0}`)))
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestServerPostRunRejectsInvalidBlueprint(t *testing.T) {
	server := NewEngineServer(&stubRunManager{submitFn: func(req RunRequest) (RunResult, error) {
		t.Fatal("submit should not be called for invalid blueprint")
		return RunResult{}, nil
	}}, "")

	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader([]byte(`{"app_id":22,"blueprint_json":{"id":"bad","version":1,"nodes":[{"id":"judge","type":"human_judge","config":{"prompt":"Resolve this.","allowed_responders":["creator"],"timeout_seconds":3600}}],"edges":[]}}`)))
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestServerPostRunReturnsConflictForDuplicate(t *testing.T) {
	server := NewEngineServer(&stubRunManager{submitFn: func(req RunRequest) (RunResult, error) {
		return RunResult{}, &duplicateRunError{RunID: "existing-run"}
	}}, "")

	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader([]byte(`{"app_id":22,"blueprint_json":{"id":"bp","version":1,"nodes":[{"id":"judge","type":"human_judge","config":{"prompt":"Resolve this.","allowed_responders":["creator"],"timeout_seconds":3600}},{"id":"submit","type":"submit_result","config":{"outcome_key":"judge.outcome"}}],"edges":[{"from":"judge","to":"submit"}]}}`)))
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusConflict)
	}
}

func TestServerGetRunReturnsState(t *testing.T) {
	server := NewEngineServer(&stubRunManager{getFn: func(runID string) (RunResult, bool) {
		return RunResult{RunID: runID, Status: RunStatusRunning, RunState: &dag.RunState{ID: runID}}, true
	}}, "")

	req := httptest.NewRequest(http.MethodGet, "/runs/run-23", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestServerDeleteRunCancels(t *testing.T) {
	server := NewEngineServer(&stubRunManager{cancelFn: func(runID string) (RunResult, bool) {
		return RunResult{RunID: runID, Status: RunStatusRunning}, true
	}}, "")

	req := httptest.NewRequest(http.MethodDelete, "/runs/run-24", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestServerPostSignalDispatchesToManager(t *testing.T) {
	server := NewEngineServer(&stubRunManager{signalFn: func(req signalRequest) (signalResult, error) {
		if req.IdempotencyKey != "sig-1" {
			t.Fatalf("idempotency_key = %q, want sig-1", req.IdempotencyKey)
		}
		if req.SignalType != "human_judgment.responded" {
			t.Fatalf("signal_type = %q, want human_judgment.responded", req.SignalType)
		}
		if req.Payload["outcome"] != "1" {
			t.Fatalf("payload outcome = %q, want 1", req.Payload["outcome"])
		}
		return signalResult{RunID: "run-signal", AppID: 42, Status: RunStatusQueued}, nil
	}}, "")

	req := httptest.NewRequest(http.MethodPost, "/signals", bytes.NewReader([]byte(`{"idempotency_key":"sig-1","app_id":42,"run_id":"run-signal","signal_type":"human_judgment.responded","correlation_key":"42:run-signal:judge","payload":{"outcome":"1"}}`)))
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", w.Code, http.StatusOK, w.Body.String())
	}
}

func TestServerHealthIncludesActiveRuns(t *testing.T) {
	server := NewEngineServer(&stubRunManager{}, "")

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["active_runs"].(float64) != 2 {
		t.Fatalf("active_runs = %v, want 2", body["active_runs"])
	}
}

func TestBuildRunResultCancellation(t *testing.T) {
	req := RunRequest{RunID: "run-cancel", AppID: 25}
	result := buildRunResult(req, nil, context.Canceled)
	if result.Status != RunStatusCancelled {
		t.Fatalf("status = %q, want %q", result.Status, RunStatusCancelled)
	}
}

func TestRunManagerPostsCallbackOnTerminalResult(t *testing.T) {
	received := make(chan RunResult, 1)
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer callback-token" {
			t.Fatalf("authorization header = %q, want Bearer callback-token", got)
		}
		var result RunResult
		if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		received <- result
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	exec := &fakeRunExecutor{run: completedRun()}
	manager := NewRunManager(exec, nil, "callback-token")
	defer manager.Close()
	if _, err := manager.Submit(RunRequest{RunID: "run-callback", AppID: 26, BlueprintJSON: []byte(`{"id":"bp"}`), CallbackURL: callbackServer.URL}); err != nil {
		t.Fatal(err)
	}

	select {
	case result := <-received:
		if result.RunID != "run-callback" {
			t.Fatalf("run_id = %q, want run-callback", result.RunID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for callback")
	}
}
