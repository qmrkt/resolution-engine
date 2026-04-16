package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/qmrkt/resolution-engine/dag"
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

type blockingExecutor struct{}

func (e *blockingExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	<-ctx.Done()
	return dag.ExecutorResult{}, ctx.Err()
}

func TestRunBlueprintHonorsProvidedContextCancellation(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "runner-test-*")
	defer os.RemoveAll(tmpDir)

	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("blocking", &blockingExecutor{})
	runner := &Runner{engine: engine, dataDir: tmpDir}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := runner.RunBlueprint(401, makeBlueprint("cancel", "blocking"), nil, RunOptions{Context: ctx})
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
	if !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("expected context canceled error, got %v", err)
	}
}

type fakeTraceSink struct {
	envelopes []TraceEnvelope
}

type closeTrackingSink struct {
	closed bool
}

func (s *closeTrackingSink) Enqueue(TraceEnvelope) bool { return true }
func (s *closeTrackingSink) Close()                     { s.closed = true }

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
}

func TestRunnerCloseClosesTraceSink(t *testing.T) {
	sink := &closeTrackingSink{}
	runner := &Runner{traceSink: sink}
	runner.Close()
	if !sink.closed {
		t.Fatal("expected runner close to close trace sink")
	}
}

func TestTraceEmitterPostsToIndexerInBackground(t *testing.T) {
	received := make(chan TraceEnvelope, 1)
	server := newTestTraceServer(t, received)
	defer server.Close()

	emitter := NewTraceEmitter(server.URL, "secret-token", nil)
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
	emitter.Close()
}

func TestTraceEmitterRejectsEnqueueAfterClose(t *testing.T) {
	emitter := NewTraceEmitter("http://127.0.0.1:1", "", nil)
	if emitter == nil {
		t.Fatal("expected emitter")
	}
	emitter.Close()

	ok := emitter.Enqueue(TraceEnvelope{AppID: 1, Run: &dag.RunState{ID: "closed"}})
	if ok {
		t.Fatal("expected enqueue to fail after close")
	}
}

func TestTraceEmitterCloseUnblocksSlowPost(t *testing.T) {
	started := make(chan struct{}, 1)
	emitter := NewTraceEmitter("http://trace.test", "", nil)
	if emitter == nil {
		t.Fatal("expected emitter")
	}
	emitter.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		started <- struct{}{}
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}

	if ok := emitter.Enqueue(TraceEnvelope{
		AppID:    404,
		Revision: 1,
		Run: &dag.RunState{
			ID:        "slow-post",
			Status:    "running",
			StartedAt: time.Now().UTC().Format(time.RFC3339),
		},
	}); !ok {
		t.Fatal("expected enqueue to succeed")
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for trace request to start")
	}

	done := make(chan struct{})
	go func() {
		emitter.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("trace emitter close did not return promptly")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
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
