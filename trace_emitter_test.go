package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/qmrkt/resolution-engine/dag"
)

// fakeTraceSink captures envelopes in-memory so tests can assert on them
// without standing up an HTTP server.
type fakeTraceSink struct {
	mu        sync.Mutex
	envelopes []TraceEnvelope
	accept    bool
}

func newFakeTraceSink() *fakeTraceSink {
	return &fakeTraceSink{accept: true}
}

func (s *fakeTraceSink) Enqueue(envelope TraceEnvelope) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.accept {
		return false
	}
	s.envelopes = append(s.envelopes, envelope)
	return true
}

func (s *fakeTraceSink) Snapshot() []TraceEnvelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]TraceEnvelope, len(s.envelopes))
	copy(out, s.envelopes)
	return out
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func TestNewTraceEmitterWithoutIndexerURLReturnsNil(t *testing.T) {
	emitter := NewTraceEmitter("", "", nil)
	if emitter != nil {
		t.Fatalf("expected nil emitter when indexer URL is empty; got %v", emitter)
	}
	if emitter.Enqueue(TraceEnvelope{}) {
		t.Fatal("nil emitter should refuse enqueue")
	}
	emitter.Close()
}

func TestTraceEmitterPostsToIndexerInBackground(t *testing.T) {
	received := make(chan TraceEnvelope, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		if envelope.Revision != 1 {
			t.Fatalf("revision = %d, want 1", envelope.Revision)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for trace POST")
	}
}

func TestTraceEmitterRejectsEnqueueAfterClose(t *testing.T) {
	emitter := NewTraceEmitter("http://127.0.0.1:1", "", nil)
	if emitter == nil {
		t.Fatal("expected non-nil emitter")
	}
	emitter.Close()

	if ok := emitter.Enqueue(TraceEnvelope{AppID: 1, Run: &dag.RunState{ID: "closed"}}); ok {
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
