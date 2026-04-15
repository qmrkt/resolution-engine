package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

type durableStubbornExecutor struct {
	started     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
	outputs     map[string]string
}

func (e *durableStubbornExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	select {
	case e.started <- struct{}{}:
	default:
	}
	<-e.release
	return dag.ExecutorResult{Outputs: cloneStringMap(e.outputs)}, nil
}

func (e *durableStubbornExecutor) Release() {
	e.releaseOnce.Do(func() {
		close(e.release)
	})
}

func setDurableBeforeSaveRun(t *testing.T, manager *DurableRunManager, hook func(*durableRunRecord) error) {
	t.Helper()
	manager.store.mu.Lock()
	manager.store.beforeSaveRun = hook
	manager.store.mu.Unlock()
}

func setDurableBeforeAppendLog(t *testing.T, manager *DurableRunManager, hook func(runID string, eventType string, payload interface{}) error) {
	t.Helper()
	manager.store.mu.Lock()
	manager.store.beforeAppendLog = hook
	manager.store.mu.Unlock()
}

func waitForDurableNotProcessing(t *testing.T, manager *DurableRunManager, runID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		manager.mu.Lock()
		processing := manager.processing[runID]
		manager.mu.Unlock()
		if !processing {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("run %q was still marked processing", runID)
}

func TestDurableStoreRejectsStaleTerminalOverwrite(t *testing.T) {
	store, err := newDurableFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	record := &durableRunRecord{
		Request:   RunRequest{RunID: "stale-terminal", AppID: 3001},
		Result:    RunResult{RunID: "stale-terminal", AppID: 3001, Status: RunStatusQueued},
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := store.saveRun(record); err != nil {
		t.Fatal(err)
	}

	first, err := store.loadRun("stale-terminal")
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.loadRun("stale-terminal")
	if err != nil {
		t.Fatal(err)
	}
	first.Result.Status = RunStatusCompleted
	first.CompletedAt = now
	if err := store.saveRun(first); err != nil {
		t.Fatal(err)
	}

	second.Result.Status = RunStatusCancelled
	second.CompletedAt = now
	if err := store.saveRun(second); !errors.Is(err, errDurableStaleRecord) {
		t.Fatalf("stale terminal save error = %v, want %v", err, errDurableStaleRecord)
	}
	final, err := store.loadRun("stale-terminal")
	if err != nil {
		t.Fatal(err)
	}
	if final.Result.Status != RunStatusCompleted {
		t.Fatalf("final status = %q, want completed", final.Result.Status)
	}
}

func TestDurableStaleWorkerCannotOverwriteCancelledRun(t *testing.T) {
	tmpDir := t.TempDir()
	stubborn := &durableStubbornExecutor{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		outputs: map[string]string{"status": "success", "outcome": "1"},
	}
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(map[string]dag.NodeExecutor{
		"outcome": stubborn,
	}), DurableRunManagerConfig{MaxWorkers: 1, PollInterval: 10 * time.Millisecond})
	defer manager.Close()
	defer stubborn.Release()

	if _, err := manager.Submit(RunRequest{RunID: "cancel-stale-worker", AppID: 3002, BlueprintJSON: simpleSubmitBlueprint("outcome")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stubborn.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stubborn worker")
	}
	cancelled, ok := manager.Cancel("cancel-stale-worker")
	if !ok {
		t.Fatal("cancel did not find run")
	}
	if cancelled.Status != RunStatusCancelled {
		t.Fatalf("cancel status = %q, want cancelled", cancelled.Status)
	}

	stubborn.Release()
	waitForDurableNotProcessing(t, manager, "cancel-stale-worker")

	final, ok := manager.Get("cancel-stale-worker")
	if !ok {
		t.Fatal("run disappeared after stale worker completion")
	}
	if final.Status != RunStatusCancelled {
		t.Fatalf("final status = %q, want cancelled", final.Status)
	}
	events, err := manager.store.loadEvents("cancel-stale-worker")
	if err != nil {
		t.Fatal(err)
	}
	if got := countDurableEvents(events, "NodeCompleted"); got == 0 {
		t.Fatalf("expected stale worker to reach node completion attempt; events=%+v", events)
	}
	if got := countDurableEvents(events, "RunCompleted"); got != 0 {
		t.Fatalf("stale worker emitted RunCompleted %d times; events=%+v", got, events)
	}
}

func TestDurableEventLogFailureDoesNotFailRun(t *testing.T) {
	tmpDir := t.TempDir()
	outcome := &durableStaticExecutor{outputs: map[string]string{"status": "success", "outcome": "1"}}
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(map[string]dag.NodeExecutor{
		"outcome": outcome,
	}), DurableRunManagerConfig{MaxWorkers: 1, PollInterval: 10 * time.Millisecond})
	defer manager.Close()
	setDurableBeforeAppendLog(t, manager, func(runID string, eventType string, payload interface{}) error {
		return errors.New("event log unavailable")
	})

	if _, err := manager.Submit(RunRequest{RunID: "event-log-down", AppID: 3003, BlueprintJSON: simpleSubmitBlueprint("outcome")}); err != nil {
		t.Fatal(err)
	}
	result := waitForDurableStatus(t, manager, "event-log-down", RunStatusCompleted)
	if result.Outcome != "1" {
		t.Fatalf("outcome = %q, want 1", result.Outcome)
	}
	events, err := manager.store.loadEvents("event-log-down")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("events were written despite injected append failure: %+v", events)
	}
}

func TestDurableCheckpointSaveFailureAfterNodeCompletedFailsFromStoredCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	outcome := &durableStaticExecutor{outputs: map[string]string{"status": "success", "outcome": "1"}}
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(map[string]dag.NodeExecutor{
		"outcome": outcome,
	}), DurableRunManagerConfig{MaxWorkers: 1, PollInterval: 10 * time.Millisecond})
	defer manager.Close()

	var failed atomic.Bool
	setDurableBeforeSaveRun(t, manager, func(record *durableRunRecord) error {
		if record.Request.RunID == "checkpoint-save-fail" &&
			record.Result.Status == RunStatusRunning &&
			record.Checkpoint.Completed["step"] &&
			failed.CompareAndSwap(false, true) {
			return errors.New("checkpoint flush denied")
		}
		return nil
	})

	if _, err := manager.Submit(RunRequest{RunID: "checkpoint-save-fail", AppID: 3004, BlueprintJSON: simpleSubmitBlueprint("outcome")}); err != nil {
		t.Fatal(err)
	}
	result := waitForDurableStatus(t, manager, "checkpoint-save-fail", RunStatusFailed)
	if !strings.Contains(result.Error, "checkpoint flush denied") {
		t.Fatalf("failure error = %q, want injected checkpoint error", result.Error)
	}
	events, err := manager.store.loadEvents("checkpoint-save-fail")
	if err != nil {
		t.Fatal(err)
	}
	if got := countDurableEvents(events, "NodeCompleted"); got == 0 {
		t.Fatalf("expected NodeCompleted audit event before checkpoint failure; events=%+v", events)
	}
	record, err := manager.store.loadRun("checkpoint-save-fail")
	if err != nil {
		t.Fatal(err)
	}
	if record.Checkpoint.Completed["step"] {
		t.Fatalf("failed checkpoint was persisted as completed: %+v", record.Checkpoint.Completed)
	}
}

func TestDurableCallbackDeliverySaveFailureRetriesPost(t *testing.T) {
	tmpDir := t.TempDir()
	var attempts atomic.Int32
	callback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer callback.Close()

	outcome := &durableStaticExecutor{outputs: map[string]string{"status": "success", "outcome": "1"}}
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(map[string]dag.NodeExecutor{
		"outcome": outcome,
	}), DurableRunManagerConfig{MaxWorkers: 1, PollInterval: 10 * time.Millisecond})
	defer manager.Close()

	var failed atomic.Bool
	setDurableBeforeSaveRun(t, manager, func(record *durableRunRecord) error {
		if record.Request.RunID == "callback-delivery-save-fail" &&
			record.CallbackDelivered &&
			failed.CompareAndSwap(false, true) {
			return errors.New("callback delivery marker denied")
		}
		return nil
	})

	if _, err := manager.Submit(RunRequest{
		RunID:         "callback-delivery-save-fail",
		AppID:         3005,
		BlueprintJSON: simpleSubmitBlueprint("outcome"),
		CallbackURL:   callback.URL,
	}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "callback-delivery-save-fail", RunStatusCompleted)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		record, err := manager.store.loadRun("callback-delivery-save-fail")
		if err != nil {
			t.Fatal(err)
		}
		if record.CallbackDelivered {
			if attempts.Load() < 2 {
				t.Fatalf("callback delivered after %d attempt(s), want retry after marker save failure", attempts.Load())
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("callback was not delivered after marker save failure; attempts=%d", attempts.Load())
}

func TestDurableRecoveryIgnoresCorruptEventLog(t *testing.T) {
	tmpDir := t.TempDir()
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(nil), DurableRunManagerConfig{
		MaxWorkers:   1,
		PollInterval: 10 * time.Millisecond,
	})
	if _, err := manager.Submit(RunRequest{RunID: "corrupt-events", AppID: 3006, BlueprintJSON: awaitSignalBlueprint()}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "corrupt-events", RunStatusWaiting)
	manager.Close()

	eventPath := filepath.Join(tmpDir, "store", "events", "corrupt-events.jsonl")
	if err := os.WriteFile(eventPath, []byte("{not-json\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	manager = newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(nil), DurableRunManagerConfig{
		MaxWorkers:   1,
		PollInterval: 10 * time.Millisecond,
	})
	defer manager.Close()
	result, ok := manager.Get("corrupt-events")
	if !ok {
		t.Fatal("run missing after recovery with corrupt event log")
	}
	if result.Status != RunStatusWaiting {
		t.Fatalf("status after recovery = %q, want waiting", result.Status)
	}
}
