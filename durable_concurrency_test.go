package main

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/qmrkt/resolution-engine/dag"
)

type durableConcurrentSubmitResult struct {
	result RunResult
	err    error
}

type durableConcurrentSignalResult struct {
	result signalResult
	err    error
}

func waitForDurableTerminal(t *testing.T, manager *DurableRunManager, runID string) RunResult {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last RunResult
	found := false
	for time.Now().Before(deadline) {
		result, ok := manager.Get(runID)
		if ok {
			found = true
			last = result
			if isTerminalStatus(result.Status) {
				return result
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !found {
		t.Fatalf("run %q never appeared", runID)
	}
	t.Fatalf("run %q did not reach terminal status; last status %q result %+v", runID, last.Status, last)
	return RunResult{}
}

func countDurableEvents(events []durableEvent, eventType string) int {
	count := 0
	for _, event := range events {
		if event.Type == eventType {
			count++
		}
	}
	return count
}

func countDurableNodeTraces(run *dag.RunState, nodeID string) int {
	if run == nil {
		return 0
	}
	count := 0
	for _, trace := range run.NodeTraces {
		if trace.NodeID == nodeID {
			count++
		}
	}
	return count
}

func TestDurableConcurrentUniqueAppSubmitsCompleteAll(t *testing.T) {
	tmpDir := t.TempDir()
	const attempts = 12
	outcome := &durableStaticExecutor{outputs: map[string]string{"status": "success", "outcome": "1"}}
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(map[string]dag.NodeExecutor{
		"outcome": outcome,
	}), DurableRunManagerConfig{
		MaxWorkers:   6,
		MaxQueueSize: attempts,
		PollInterval: 5 * time.Millisecond,
	})
	defer manager.Close()

	start := make(chan struct{})
	results := make([]durableConcurrentSubmitResult, attempts)
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			runID := fmt.Sprintf("concurrent-unique-%02d", i)
			result, err := manager.Submit(RunRequest{
				RunID:         runID,
				AppID:         2000 + i,
				BlueprintJSON: simpleSubmitBlueprint("outcome"),
			})
			results[i] = durableConcurrentSubmitResult{result: result, err: err}
		}(i)
	}
	close(start)
	wg.Wait()

	for i, submit := range results {
		if submit.err != nil {
			t.Fatalf("submit %d failed: %v", i, submit.err)
		}
		if submit.result.RunID == "" {
			t.Fatalf("submit %d returned empty run id", i)
		}
	}
	for i := 0; i < attempts; i++ {
		runID := fmt.Sprintf("concurrent-unique-%02d", i)
		result := waitForDurableTerminal(t, manager, runID)
		if result.Status != RunStatusCompleted {
			t.Fatalf("%s status = %q, want completed", runID, result.Status)
		}
		if result.Outcome != "1" {
			t.Fatalf("%s outcome = %q, want 1", runID, result.Outcome)
		}
	}
	if got := outcome.Count(); got != attempts {
		t.Fatalf("outcome executor count = %d, want %d", got, attempts)
	}
	if active := manager.ActiveCount(); active != 0 {
		t.Fatalf("active count after all complete = %d, want 0", active)
	}
}

func TestDurableConcurrentSameAppSubmitsAdmitOneRun(t *testing.T) {
	tmpDir := t.TempDir()
	const attempts = 32
	const appID = 2100
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(nil), DurableRunManagerConfig{
		MaxWorkers:   4,
		MaxQueueSize: attempts,
		PollInterval: 5 * time.Millisecond,
	})
	defer manager.Close()

	start := make(chan struct{})
	results := make([]durableConcurrentSubmitResult, attempts)
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			result, err := manager.Submit(RunRequest{
				RunID:         fmt.Sprintf("same-app-%02d", i),
				AppID:         appID,
				BlueprintJSON: awaitSignalBlueprint(),
			})
			results[i] = durableConcurrentSubmitResult{result: result, err: err}
		}(i)
	}
	close(start)
	wg.Wait()

	var accepted []RunResult
	duplicates := 0
	for i, submit := range results {
		if submit.err == nil {
			accepted = append(accepted, submit.result)
			continue
		}
		var dup *duplicateRunError
		if !errors.As(submit.err, &dup) {
			t.Fatalf("submit %d error = %v, want duplicateRunError", i, submit.err)
		}
		duplicates++
	}
	if len(accepted) != 1 {
		t.Fatalf("accepted runs = %d, want 1; results=%+v", len(accepted), results)
	}
	if duplicates != attempts-1 {
		t.Fatalf("duplicate errors = %d, want %d", duplicates, attempts-1)
	}
	waitForDurableStatus(t, manager, accepted[0].RunID, RunStatusWaiting)

	runs, err := manager.store.listRuns()
	if err != nil {
		t.Fatal(err)
	}
	appRuns := 0
	for _, run := range runs {
		if run.Request.AppID == appID {
			appRuns++
		}
	}
	if appRuns != 1 {
		t.Fatalf("stored runs for app %d = %d, want 1", appID, appRuns)
	}
}

func TestDurableConcurrentDuplicateSignalsAreIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	const attempts = 32
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(nil), DurableRunManagerConfig{
		MaxWorkers:   4,
		MaxQueueSize: attempts,
		PollInterval: 5 * time.Millisecond,
	})
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "concurrent-signal", AppID: 2200, BlueprintJSON: awaitSignalBlueprint()}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "concurrent-signal", RunStatusWaiting)

	start := make(chan struct{})
	results := make([]durableConcurrentSignalResult, attempts)
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			result, err := manager.Signal(signalRequest{
				IdempotencyKey: "same-signal-key",
				AppID:          2200,
				RunID:          "concurrent-signal",
				SignalType:     "human_judgment.responded",
				CorrelationKey: "2200:concurrent-signal:judge",
				Payload:        map[string]string{"outcome": "1", "reason": "concurrent"},
			})
			results[i] = durableConcurrentSignalResult{result: result, err: err}
		}(i)
	}
	close(start)
	wg.Wait()

	for i, signal := range results {
		if signal.err != nil {
			t.Fatalf("signal %d failed: %v", i, signal.err)
		}
		if signal.result.RunID != "concurrent-signal" {
			t.Fatalf("signal %d run id = %q, want concurrent-signal", i, signal.result.RunID)
		}
	}
	result := waitForDurableStatus(t, manager, "concurrent-signal", RunStatusCompleted)
	if result.Outcome != "1" {
		t.Fatalf("outcome = %q, want 1", result.Outcome)
	}
	if got := countDurableNodeTraces(result.RunState, "judge"); got != 1 {
		t.Fatalf("judge trace count = %d, want 1", got)
	}

	record, err := manager.store.loadRun("concurrent-signal")
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Signals) != 1 {
		t.Fatalf("stored signal count = %d, want 1: %+v", len(record.Signals), record.Signals)
	}
	events, err := manager.store.loadEvents("concurrent-signal")
	if err != nil {
		t.Fatal(err)
	}
	if got := countDurableEvents(events, "SignalReceived"); got != 1 {
		t.Fatalf("SignalReceived event count = %d, want 1; events=%+v", got, events)
	}
}

func TestDurableConcurrentCancelAndSignalsLeaveTerminalRunClean(t *testing.T) {
	tmpDir := t.TempDir()
	const signals = 24
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(nil), DurableRunManagerConfig{
		MaxWorkers:   4,
		MaxQueueSize: signals,
		PollInterval: 5 * time.Millisecond,
	})
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "cancel-signal-race", AppID: 2300, BlueprintJSON: awaitSignalBlueprint()}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "cancel-signal-race", RunStatusWaiting)

	start := make(chan struct{})
	var wg sync.WaitGroup
	cancelResult := make(chan RunResult, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		result, ok := manager.Cancel("cancel-signal-race")
		if !ok {
			t.Error("cancel did not find run")
			return
		}
		cancelResult <- result
	}()
	for i := 0; i < signals; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := manager.Signal(signalRequest{
				IdempotencyKey: fmt.Sprintf("cancel-race-signal-%02d", i),
				AppID:          2300,
				RunID:          "cancel-signal-race",
				SignalType:     "human_judgment.responded",
				CorrelationKey: "2300:cancel-signal-race:judge",
				Payload:        map[string]string{"outcome": "1"},
			})
			if err != nil {
				t.Errorf("signal %d failed: %v", i, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(cancelResult)

	cancelledByCancel := false
	for result := range cancelResult {
		if !isTerminalStatus(result.Status) {
			t.Fatalf("cancel returned status %q, want terminal", result.Status)
		}
		if result.Status == RunStatusCancelled {
			cancelledByCancel = true
		}
	}
	result := waitForDurableTerminal(t, manager, "cancel-signal-race")
	if cancelledByCancel && result.Status != RunStatusCancelled {
		t.Fatalf("final status = %q after cancel returned cancelled, want cancelled", result.Status)
	}
	if result.Status != RunStatusCompleted && result.Status != RunStatusCancelled {
		t.Fatalf("final status = %q, want completed or cancelled", result.Status)
	}
	if active := manager.ActiveCount(); active != 0 {
		t.Fatalf("active count after cancel/signal race = %d, want 0", active)
	}

	record, err := manager.store.loadRun("cancel-signal-race")
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Checkpoint.Waiting) != 0 {
		t.Fatalf("terminal run still has waiting nodes: %+v", record.Checkpoint.Waiting)
	}
}

// TestDurableEnqueueRaceWithBusyWorkerIsNotLost covers the lost-wake-up race
// between an enqueue (from a signal or timer) and a worker that is already
// processing the same run. Before the fix, an idle worker would dequeue the
// runID, discover markProcessing returns false because another worker holds
// the mark, and silently drop the dispatch. If that dropped dispatch carried
// the only signal that could progress the run, the run was effectively stuck.
//
// The fix: markProcessing sets a pending flag when contention is detected,
// and the worker that holds the mark re-enqueues the run on its way out via
// unmarkProcessing. PollInterval is set long enough that the timer loop is
// not helping on its own — the enqueue must be preserved by the pending-flag
// machinery for the run to make progress.
func TestDurableEnqueueRaceWithBusyWorkerIsNotLost(t *testing.T) {
	tmpDir := t.TempDir()
	blocking := newDurableReleaseExecutor(map[string]string{"status": "success", "outcome": "1"})
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(map[string]dag.NodeExecutor{
		"blocking": blocking,
	}), DurableRunManagerConfig{
		MaxWorkers:   4,
		MaxQueueSize: 32,
		PollInterval: 10 * time.Second,
	})
	defer manager.Close()

	const runID = "race-run"
	if _, err := manager.Submit(RunRequest{
		RunID:         runID,
		AppID:         2400,
		BlueprintJSON: simpleSubmitBlueprint("blocking"),
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-blocking.started:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for blocking node to start")
	}

	// One worker is now holding markProcessing(runID) inside exec.Execute.
	// Simulate rapid-fire enqueues — as would arise from a signal handler or
	// timer loop firing repeatedly while the worker is busy.
	for i := 0; i < 8; i++ {
		manager.enqueue(runID)
	}

	// Idle workers must dequeue these, fail to claim, and leave a pending
	// breadcrumb so the holder re-enqueues on exit.
	deadline := time.Now().Add(2 * time.Second)
	var hadPending bool
	for time.Now().Before(deadline) {
		manager.mu.Lock()
		hadPending = manager.pending[runID]
		manager.mu.Unlock()
		if hadPending {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !hadPending {
		t.Fatal("expected pending flag to be set while worker held markProcessing")
	}

	// Release the holder. The run completes normally; the pending flag
	// triggers a benign re-enqueue against a terminal run.
	close(blocking.release)
	result := waitForDurableStatus(t, manager, runID, RunStatusCompleted)
	if result.Outcome != "1" {
		t.Fatalf("outcome = %q, want 1", result.Outcome)
	}
}

// TestDurableEnqueueBurstStaysBounded verifies the enqueue dedup. With
// MaxWorkers=1 and MaxQueueSize=1, a burst of enqueues targeting a run that
// is already being processed would — without dedup — see the channel fill
// up on the first call and spawn a blocked sender goroutine for every
// subsequent call. With dedup, all but the first see processing=true and
// return immediately without touching the channel.
func TestDurableEnqueueBurstStaysBounded(t *testing.T) {
	tmpDir := t.TempDir()
	blocking := newDurableReleaseExecutor(map[string]string{"status": "success", "outcome": "1"})
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(map[string]dag.NodeExecutor{
		"blocking": blocking,
	}), DurableRunManagerConfig{
		MaxWorkers:   1,
		MaxQueueSize: 1,
		PollInterval: 10 * time.Second,
	})
	defer manager.Close()

	const runID = "enqueue-burst"
	if _, err := manager.Submit(RunRequest{
		RunID:         runID,
		AppID:         2500,
		BlueprintJSON: simpleSubmitBlueprint("blocking"),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocking.started:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for blocking node to start")
	}

	// Let any transient startup goroutines settle, then sample baseline.
	time.Sleep(30 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	const burst = 200
	for i := 0; i < burst; i++ {
		manager.enqueue(runID)
	}
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()
	if diff := after - baseline; diff > 20 {
		t.Fatalf("goroutine count grew by %d after a %d-enqueue burst (before=%d, after=%d) — dedup regressed", diff, burst, baseline, after)
	}

	// Confirm the dedup path actually fired: pending should be set because
	// the worker is still busy with the blocking executor.
	manager.mu.Lock()
	pendingSet := manager.pending[runID]
	manager.mu.Unlock()
	if !pendingSet {
		t.Fatal("pending flag was not set during enqueue burst — dedup did not exercise the processing path")
	}

	close(blocking.release)
	waitForDurableStatus(t, manager, runID, RunStatusCompleted)
}
