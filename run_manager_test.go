package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

type fakeRunExecutor struct {
	mu       sync.Mutex
	run      *dag.RunState
	err      error
	block    bool
	runCount int
}

func (f *fakeRunExecutor) RunBlueprint(appID int, resolutionLogicJSON []byte, inputs map[string]string, opts ...RunOptions) (*dag.RunState, error) {
	f.mu.Lock()
	f.runCount++
	shouldBlock := f.block
	f.mu.Unlock()
	if shouldBlock {
		ctx := firstRunOptions(opts...).Context
		if ctx == nil {
			ctx = context.Background()
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.run, f.err
}

func (f *fakeRunExecutor) WriteEvidence(appID int, payload interface{}) (string, error) {
	return "evidence.json", nil
}

func TestRunManagerSubmitAndCompleteLifecycle(t *testing.T) {
	exec := &fakeRunExecutor{run: &dag.RunState{Status: "completed", Context: map[string]string{"submit.outcome": "1", "submit.evidence_hash": "abc", "submit.submitted": "true"}}}
	manager := NewRunManagerWithConfig(exec, nil, "", time.Hour, time.Hour)
	defer manager.Close()

	accepted, err := manager.Submit(RunRequest{RunID: "run-1", AppID: 11, BlueprintJSON: []byte(`{"id":"bp"}`), BlueprintPath: PathMain})
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Status != RunStatusAccepted {
		t.Fatalf("accepted status = %q, want %q", accepted.Status, RunStatusAccepted)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		result, ok := manager.Get("run-1")
		if ok && result.Status == RunStatusCompleted {
			if result.Action != RunActionPropose {
				t.Fatalf("action = %q, want %q", result.Action, RunActionPropose)
			}
			if result.Outcome != "1" || result.EvidenceHash != "abc" {
				t.Fatalf("unexpected terminal result: %+v", result)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for terminal result")
}

func TestRunManagerRejectsDuplicateInflightAppID(t *testing.T) {
	exec := &fakeRunExecutor{block: true}
	manager := NewRunManager(exec, nil, "")
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "run-a", AppID: 12, BlueprintJSON: []byte(`{"id":"bp"}`)}); err != nil {
		t.Fatal(err)
	}
	_, err := manager.Submit(RunRequest{RunID: "run-b", AppID: 12, BlueprintJSON: []byte(`{"id":"bp"}`)})
	var dup *duplicateRunError
	if !errors.As(err, &dup) {
		t.Fatalf("expected duplicateRunError, got %v", err)
	}
	if dup.RunID != "run-a" {
		t.Fatalf("duplicate run id = %q, want run-a", dup.RunID)
	}
}

func TestRunManagerCancelTransitionsToCancelled(t *testing.T) {
	exec := &fakeRunExecutor{block: true}
	manager := NewRunManager(exec, nil, "")
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "run-cancel", AppID: 13, BlueprintJSON: []byte(`{"id":"bp"}`)}); err != nil {
		t.Fatal(err)
	}
	if _, ok := manager.Cancel("run-cancel"); !ok {
		t.Fatal("expected cancel to find run")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		result, ok := manager.Get("run-cancel")
		if ok && result.Status == RunStatusCancelled {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for cancelled status")
}

func TestRunManagerCleansUpExpiredTerminalRuns(t *testing.T) {
	exec := &fakeRunExecutor{run: &dag.RunState{Status: "completed"}}
	manager := NewRunManagerWithConfig(exec, nil, "", 10*time.Millisecond, time.Hour)
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "run-expire", AppID: 14, BlueprintJSON: []byte(`{"id":"bp"}`)}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	manager.cleanupExpired()
	if _, ok := manager.Get("run-expire"); ok {
		t.Fatal("expected terminal run to be cleaned up")
	}
}
