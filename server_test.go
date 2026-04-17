package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/qmrkt/resolution-engine/dag"
)

type stubRunManager struct {
	submitFn func(RunRequest) (RunResult, error)
	getFn    func(string) (RunResult, bool)
	cancelFn func(string) (RunResult, bool)
	signalFn func(signalRequest) (signalResult, error)
}

func validRunRequestBody() []byte {
	return []byte(`{"app_id":21,"blueprint_json":{"id":"bp","version":1,"nodes":[{"id":"judge","type":"await_signal","config":{"signal_type":"human_judgment.responded","required_payload":["outcome"],"default_outputs":{"status":"responded"},"timeout_seconds":3600}},{"id":"done","type":"return","config":{"value":{"status":"success","outcome":"{{results.judge.outcome}}"}}}],"edges":[{"from":"judge","to":"done"}]}}`)
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
		return RunResult{RunID: req.RunID, AppID: 21, Status: RunStatusQueued}, nil
	}}, "")

	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(validRunRequestBody()))
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

	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader([]byte(`{"app_id":22,"blueprint_json":{"id":"bad","version":1,"nodes":[{"id":"judge","type":"await_signal","config":{"signal_type":"human_judgment.responded","required_payload":["outcome"],"default_outputs":{"status":"responded"},"timeout_seconds":3600}}],"edges":[]}}`)))
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

	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(validRunRequestBody()))
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

func TestServerMapsManagerShutdownErrorsToServiceUnavailable(t *testing.T) {
	t.Run("run submit", func(t *testing.T) {
		server := NewEngineServer(&stubRunManager{submitFn: func(req RunRequest) (RunResult, error) {
			return RunResult{}, errDurableManagerShuttingDown
		}}, "")
		req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(validRunRequestBody()))
		w := httptest.NewRecorder()
		server.Handler().ServeHTTP(w, req)
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
		}
	})

	t.Run("signal", func(t *testing.T) {
		server := NewEngineServer(&stubRunManager{signalFn: func(req signalRequest) (signalResult, error) {
			return signalResult{}, errDurableManagerShuttingDown
		}}, "")
		req := httptest.NewRequest(http.MethodPost, "/signals", bytes.NewReader([]byte(`{"idempotency_key":"sig-1","app_id":42,"run_id":"run-signal","signal_type":"human_judgment.responded","correlation_key":"42:run-signal:judge","payload":{"outcome":"1"}}`)))
		w := httptest.NewRecorder()
		server.Handler().ServeHTTP(w, req)
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
		}
	})
}

func TestBuildRunResultCancellation(t *testing.T) {
	req := RunRequest{RunID: "run-cancel", AppID: 25}
	result := buildRunResult(req, nil, context.Canceled)
	if result.Status != RunStatusCancelled {
		t.Fatalf("status = %q, want %q", result.Status, RunStatusCancelled)
	}
}

func TestServerRejectsMutatingRequestsDuringShutdown(t *testing.T) {
	server := NewEngineServer(&stubRunManager{
		submitFn: func(RunRequest) (RunResult, error) {
			t.Fatal("Submit reached while shutting down")
			return RunResult{}, nil
		},
		cancelFn: func(string) (RunResult, bool) {
			t.Fatal("Cancel reached while shutting down")
			return RunResult{}, false
		},
		signalFn: func(signalRequest) (signalResult, error) {
			t.Fatal("Signal reached while shutting down")
			return signalResult{}, nil
		},
	}, "")
	server.BeginShutdown()

	cases := []struct {
		name   string
		method string
		path   string
		body   []byte
	}{
		{name: "submit", method: http.MethodPost, path: "/run", body: validRunRequestBody()},
		{name: "signal", method: http.MethodPost, path: "/signals", body: []byte(`{"idempotency_key":"sig-1","app_id":42,"run_id":"run-signal","signal_type":"human_judgment.responded","correlation_key":"42:run-signal:judge","payload":{"outcome":"1"}}`)},
		{name: "cancel", method: http.MethodDelete, path: "/runs/run-24"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, bytes.NewReader(tc.body))
			w := httptest.NewRecorder()
			server.Handler().ServeHTTP(w, req)
			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
			}
		})
	}
}

func TestServerAllowsReadOnlyRequestsDuringShutdown(t *testing.T) {
	server := NewEngineServer(&stubRunManager{getFn: func(runID string) (RunResult, bool) {
		return RunResult{RunID: runID, Status: RunStatusRunning, RunState: &dag.RunState{ID: runID}}, true
	}}, "")
	server.BeginShutdown()

	getReq := httptest.NewRequest(http.MethodGet, "/runs/run-23", nil)
	getResp := httptest.NewRecorder()
	server.Handler().ServeHTTP(getResp, getReq)
	if getResp.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", getResp.Code, http.StatusOK)
	}

	healthReq := httptest.NewRequest(http.MethodGet, "/health", nil)
	healthResp := httptest.NewRecorder()
	server.Handler().ServeHTTP(healthResp, healthReq)
	if healthResp.Code != http.StatusOK {
		t.Fatalf("health status = %d, want %d", healthResp.Code, http.StatusOK)
	}
}

// TestServerRejectsPathTraversalRunIDs asserts the manager is never invoked
// with a malformed run_id. Some inputs are rejected by the validator (400);
// others are normalized or refused by http.ServeMux (301 or 404). Any
// non-2xx response is acceptable as long as the stub's methods are not
// called.
func TestServerRejectsPathTraversalRunIDs(t *testing.T) {
	invalid := []string{
		"../../../etc/passwd",
		"..%2F..%2Fetc%2Fpasswd",
		"run/../other",
		"has spaces",
		strings.Repeat("x", 129),
	}
	fatalIfCalled := &stubRunManager{
		getFn: func(string) (RunResult, bool) {
			t.Fatal("Get reached with malformed run_id")
			return RunResult{}, false
		},
		cancelFn: func(string) (RunResult, bool) {
			t.Fatal("Cancel reached with malformed run_id")
			return RunResult{}, false
		},
	}
	server := NewEngineServer(fatalIfCalled, "")
	for _, bad := range invalid {
		for _, method := range []string{http.MethodGet, http.MethodDelete} {
			name := method + "/" + bad
			t.Run(name, func(t *testing.T) {
				req, err := http.NewRequest(method, "/runs/"+bad, nil)
				if err != nil {
					t.Fatal(err)
				}
				w := httptest.NewRecorder()
				server.Handler().ServeHTTP(w, req)
				if w.Code >= 200 && w.Code < 300 {
					t.Fatalf("%s /runs/%q status = %d, want non-2xx", method, bad, w.Code)
				}
			})
		}
	}

	t.Run("signal with malformed run_id", func(t *testing.T) {
		manager := &stubRunManager{
			signalFn: func(signalRequest) (signalResult, error) {
				t.Fatal("Signal reached with malformed run_id")
				return signalResult{}, nil
			},
		}
		sigServer := NewEngineServer(manager, "")
		body := `{"idempotency_key":"k","app_id":1,"run_id":"../../../etc/passwd","signal_type":"t","correlation_key":"c"}`
		req := httptest.NewRequest(http.MethodPost, "/signals", bytes.NewReader([]byte(body)))
		w := httptest.NewRecorder()
		sigServer.Handler().ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("POST /signals status = %d, want 400", w.Code)
		}
	})
}

// TestServerAcceptsValidRunIDs confirms the validator is not over-restrictive:
// UUIDs and dashed identifiers still reach the manager.
func TestServerAcceptsValidRunIDs(t *testing.T) {
	valid := []string{
		"550e8400-e29b-41d4-a716-446655440000",
		"run-123",
		"BUSY_RUN_01",
		"a",
	}
	for _, good := range valid {
		good := good
		t.Run("get/"+good, func(t *testing.T) {
			var seen string
			manager := &stubRunManager{getFn: func(runID string) (RunResult, bool) {
				seen = runID
				return RunResult{RunID: runID, Status: RunStatusRunning}, true
			}}
			server := NewEngineServer(manager, "")
			req := httptest.NewRequest(http.MethodGet, "/runs/"+good, nil)
			w := httptest.NewRecorder()
			server.Handler().ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Fatalf("GET /runs/%s status = %d, want 200", good, w.Code)
			}
			if seen != good {
				t.Fatalf("manager.Get got run_id = %q, want %q", seen, good)
			}
		})
	}
}
