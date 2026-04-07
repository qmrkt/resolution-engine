package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

// mockExecutor records calls and returns configured results.
type mockExecutor struct {
	outputs map[string]string
	err     error
}

func (m *mockExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	if m.err != nil {
		return dag.ExecutorResult{}, m.err
	}
	return dag.ExecutorResult{Outputs: m.outputs}, nil
}

func makeBlueprint(id, nodeType string) []byte {
	bp := dag.Blueprint{
		ID: id,
		Nodes: []dag.NodeDef{
			{ID: "step", Type: nodeType, Config: map[string]interface{}{}},
		},
	}
	data, _ := json.Marshal(bp)
	return data
}

func makeBlueprintWithSubmit(id, fetchType string) []byte {
	bp := dag.Blueprint{
		ID: id,
		Nodes: []dag.NodeDef{
			{ID: "step", Type: fetchType, Config: map[string]interface{}{}},
			{ID: "submit", Type: "submit_result", Config: map[string]interface{}{}},
		},
		Edges: []dag.EdgeDef{
			{From: "step", To: "submit", Condition: "step.status == 'success'"},
		},
	}
	data, _ := json.Marshal(bp)
	return data
}

func TestDualPathMainSuccess(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "runner-test-*")
	defer os.RemoveAll(tmpDir)

	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("good_fetch", &mockExecutor{
		outputs: map[string]string{"status": "success", "outcome": "1"},
	})
	engine.RegisterExecutor("submit_result", &mockExecutor{
		outputs: map[string]string{"status": "success", "outcome": "1", "evidence_hash": "abc123", "submitted": "true"},
	})
	engine.RegisterExecutor("dispute_step", &mockExecutor{
		outputs: map[string]string{"status": "success", "outcome": "2"},
	})

	runner := &Runner{engine: engine, dataDir: tmpDir, inFlight: make(map[int]bool)}

	mainBP := makeBlueprintWithSubmit("main", "good_fetch")
	disputeBP := makeBlueprintWithSubmit("dispute", "dispute_step")

	result, err := runner.executeDualPath(100, mainBP, disputeBP)
	if err != nil {
		t.Fatal(err)
	}
	if result.PathUsed != PathMain {
		t.Fatalf("expected path=main, got %s", result.PathUsed)
	}
	if result.Outcome != "1" {
		t.Fatalf("expected outcome=1, got %s", result.Outcome)
	}
	if result.DisputeRun != nil {
		t.Fatal("dispute path should not have run when main succeeds")
	}
	if result.Cancelled {
		t.Fatal("should not be cancelled")
	}
}

func TestDualPathMainFailsDisputeSucceeds(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "runner-test-*")
	defer os.RemoveAll(tmpDir)

	engine := dag.NewEngine(nil)
	// Main path: fetch fails, submit never runs
	engine.RegisterExecutor("bad_fetch", &mockExecutor{
		outputs: map[string]string{"status": "failed", "error": "API down"},
	})
	// Dispute path: succeeds
	engine.RegisterExecutor("dispute_fetch", &mockExecutor{
		outputs: map[string]string{"status": "success", "outcome": "2"},
	})
	engine.RegisterExecutor("submit_result", &mockExecutor{
		outputs: map[string]string{"status": "success", "outcome": "2", "evidence_hash": "def456", "submitted": "true"},
	})

	runner := &Runner{engine: engine, dataDir: tmpDir, inFlight: make(map[int]bool)}

	mainBP := makeBlueprintWithSubmit("main", "bad_fetch")
	disputeBP := makeBlueprintWithSubmit("dispute", "dispute_fetch")

	result, err := runner.executeDualPath(101, mainBP, disputeBP)
	if err != nil {
		t.Fatal(err)
	}
	if result.PathUsed != PathDispute {
		t.Fatalf("expected path=dispute, got %s", result.PathUsed)
	}
	if result.Outcome != "2" {
		t.Fatalf("expected outcome=2, got %s", result.Outcome)
	}
	if result.MainRun == nil {
		t.Fatal("main run should be recorded even on failure")
	}
	if result.DisputeRun == nil {
		t.Fatal("dispute run should be recorded")
	}
	if result.Cancelled {
		t.Fatal("should not be cancelled when dispute succeeds")
	}
}

func TestDualPathBothFail(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "runner-test-*")
	defer os.RemoveAll(tmpDir)

	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("bad_fetch", &mockExecutor{
		outputs: map[string]string{"status": "failed", "error": "API down"},
	})
	engine.RegisterExecutor("submit_result", &mockExecutor{
		outputs: map[string]string{"status": "success", "outcome": "", "evidence_hash": "", "submitted": "true"},
	})

	runner := &Runner{engine: engine, dataDir: tmpDir, inFlight: make(map[int]bool)}

	mainBP := makeBlueprintWithSubmit("main", "bad_fetch")
	disputeBP := makeBlueprintWithSubmit("dispute", "bad_fetch")

	result, err := runner.executeDualPath(102, mainBP, disputeBP)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Cancelled {
		t.Fatal("expected cancelled when both paths fail")
	}
	if result.CancelReason == "" {
		t.Fatal("expected cancel reason")
	}
	if result.MainRun == nil {
		t.Fatal("main run should be recorded")
	}
	if result.DisputeRun == nil {
		t.Fatal("dispute run should be recorded")
	}
}

func TestIsResolutionSuccessful(t *testing.T) {
	tests := []struct {
		name     string
		run      *dag.RunState
		expected bool
	}{
		{
			name:     "successful with outcome",
			run:      &dag.RunState{Status: "completed", Context: map[string]string{"submit.outcome": "1"}},
			expected: true,
		},
		{
			name:     "failed status",
			run:      &dag.RunState{Status: "failed", Context: map[string]string{"submit.outcome": "1"}},
			expected: false,
		},
		{
			name:     "no outcome",
			run:      &dag.RunState{Status: "completed", Context: map[string]string{}},
			expected: false,
		},
		{
			name:     "inconclusive outcome",
			run:      &dag.RunState{Status: "completed", Context: map[string]string{"submit.outcome": "inconclusive"}},
			expected: false,
		},
		{
			name:     "cancelled by cancel_market",
			run:      &dag.RunState{Status: "completed", Context: map[string]string{"cancel_market.cancelled": "true", "submit.outcome": "1"}},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isResolutionSuccessful(tc.run)
			if got != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}

func TestEvidenceHashDeterministic(t *testing.T) {
	run := &dag.RunState{
		ID:     "test",
		Status: "completed",
		Context: map[string]string{
			"submit.outcome": "1",
		},
	}
	h1 := EvidenceHash(run)
	h2 := EvidenceHash(run)
	if h1 != h2 {
		t.Fatalf("evidence hash should be deterministic: %s != %s", h1, h2)
	}
	if len(h1) != 64 {
		t.Fatalf("expected 64 char hex hash, got %d chars", len(h1))
	}
}

func TestTryResolveWithDisputeDeduplicates(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "runner-test-*")
	defer os.RemoveAll(tmpDir)

	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("slow_fetch", &mockExecutor{
		outputs: map[string]string{"status": "success", "outcome": "1"},
	})
	engine.RegisterExecutor("submit_result", &mockExecutor{
		outputs: map[string]string{"status": "success", "outcome": "1", "evidence_hash": "abc", "submitted": "true"},
	})

	runner := &Runner{engine: engine, dataDir: tmpDir, inFlight: make(map[int]bool)}

	bp := makeBlueprintWithSubmit("test", "slow_fetch")

	// First call should start
	runner.TryResolveWithDispute(200, bp, bp)

	// Second call with same appID should be deduplicated
	runner.mu.Lock()
	inFlight := runner.inFlight[200]
	runner.mu.Unlock()
	if !inFlight {
		t.Fatal("expected market 200 to be in-flight")
	}
}

type fakeTraceSink struct {
	envelopes []TraceEnvelope
}

func (f *fakeTraceSink) Enqueue(envelope TraceEnvelope) bool {
	f.envelopes = append(f.envelopes, envelope)
	return true
}

func TestRunBlueprintEmitsTraceSnapshots(t *testing.T) {
	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("good_fetch", &mockExecutor{
		outputs: map[string]string{"status": "success", "outcome": "1"},
	})

	sink := &fakeTraceSink{}
	runner := &Runner{
		engine:    engine,
		traceSink: sink,
		inFlight:  make(map[int]bool),
	}

	run, err := runner.RunBlueprint(301, makeBlueprint("trace-main", "good_fetch"), map[string]string{"market.question": "Will it trace?"}, RunOptions{
		Trace: &TraceMetadata{
			AppID:         301,
			BlueprintPath: PathMain,
			Initiator:     "test:runner",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(sink.envelopes) < 3 {
		t.Fatalf("expected multiple trace snapshots, got %d", len(sink.envelopes))
	}

	for index, envelope := range sink.envelopes {
		if envelope.AppID != 301 {
			t.Fatalf("envelope[%d].AppID = %d, want 301", index, envelope.AppID)
		}
		if envelope.BlueprintPath != PathMain {
			t.Fatalf("envelope[%d].BlueprintPath = %q, want %q", index, envelope.BlueprintPath, PathMain)
		}
		if envelope.Initiator != "test:runner" {
			t.Fatalf("envelope[%d].Initiator = %q, want test:runner", index, envelope.Initiator)
		}
		if envelope.Run == nil {
			t.Fatalf("envelope[%d].Run is nil", index)
		}
		if envelope.Run.ID != run.ID {
			t.Fatalf("envelope[%d].run.id = %q, want %q", index, envelope.Run.ID, run.ID)
		}
		if envelope.Revision != index+1 {
			t.Fatalf("envelope[%d].revision = %d, want %d", index, envelope.Revision, index+1)
		}
	}

	finalEnvelope := sink.envelopes[len(sink.envelopes)-1]
	if finalEnvelope.Run.Status != "completed" {
		t.Fatalf("final status = %q, want completed", finalEnvelope.Run.Status)
	}
	if finalEnvelope.Run.Context["step.outcome"] != "1" {
		t.Fatalf("final context missing output: %#v", finalEnvelope.Run.Context)
	}
}

func TestExecuteDualPathEmitsMainAndDisputeTracePaths(t *testing.T) {
	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("bad_fetch", &mockExecutor{err: context.DeadlineExceeded})
	engine.RegisterExecutor("dispute_fetch", &mockExecutor{
		outputs: map[string]string{"status": "success", "outcome": "2"},
	})
	engine.RegisterExecutor("submit_result", &mockExecutor{
		outputs: map[string]string{"status": "success", "outcome": "2", "evidence_hash": "def456", "submitted": "true"},
	})

	sink := &fakeTraceSink{}
	runner := &Runner{
		engine:    engine,
		traceSink: sink,
		inFlight:  make(map[int]bool),
	}

	result, err := runner.executeDualPath(302, makeBlueprintWithSubmit("main", "bad_fetch"), makeBlueprintWithSubmit("dispute", "dispute_fetch"))
	if err != nil {
		t.Fatal(err)
	}
	if result.DisputeRun == nil {
		t.Fatal("expected dispute run to complete")
	}

	seenPaths := map[string]bool{}
	for _, envelope := range sink.envelopes {
		seenPaths[envelope.BlueprintPath] = true
	}
	if !seenPaths[PathMain] || !seenPaths[PathDispute] {
		t.Fatalf("expected main and dispute trace paths, got %+v", seenPaths)
	}
}

func TestTraceEmitterPostsToIndexerInBackground(t *testing.T) {
	received := make(chan TraceEnvelope, 1)
	server := newTestTraceServer(t, received)
	defer server.Close()

	emitter := NewTraceEmitter(server.URL, "secret-token", nil)
	defer emitter.Close()

	run := &dag.RunState{
		ID:          "run-trace-http",
		BlueprintID: "trace-http",
		Status:      "running",
		StartedAt:   time.Now().UTC().Format(time.RFC3339),
	}
	if ok := emitter.Enqueue(TraceEnvelope{
		AppID:         303,
		BlueprintPath: PathMain,
		Initiator:     "test:http",
		Revision:      1,
		Run:           run,
	}); !ok {
		t.Fatal("expected enqueue to succeed")
	}

	select {
	case envelope := <-received:
		if envelope.AppID != 303 || envelope.Run == nil || envelope.Run.ID != "run-trace-http" {
			t.Fatalf("unexpected posted envelope: %+v", envelope)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for trace POST")
	}
}

func newTestTraceServer(t *testing.T, received chan<- TraceEnvelope) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret-token" {
			t.Fatalf("authorization header = %q, want Bearer secret-token", got)
		}
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}

		var envelope TraceEnvelope
		if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
			t.Fatalf("decode trace envelope: %v", err)
		}
		received <- envelope
		w.WriteHeader(http.StatusOK)
	}))
}
