package main

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/qmrkt/resolution-engine/dag"
)

// parallelDelayExecutor records sibling start times.
type parallelDelayExecutor struct {
	delay  time.Duration
	mu     sync.Mutex
	starts []time.Time
}

func (e *parallelDelayExecutor) Execute(ctx context.Context, node dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	e.mu.Lock()
	e.starts = append(e.starts, time.Now())
	e.mu.Unlock()
	select {
	case <-time.After(e.delay):
		return dag.ExecutorResult{Outputs: map[string]string{"status": "success"}}, nil
	case <-ctx.Done():
		return dag.ExecutorResult{}, ctx.Err()
	}
}

func (e *parallelDelayExecutor) startSpread() time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.starts) < 2 {
		return 0
	}
	min, max := e.starts[0], e.starts[0]
	for _, t := range e.starts[1:] {
		if t.Before(min) {
			min = t
		}
		if t.After(max) {
			max = t
		}
	}
	return max.Sub(min)
}

func (e *parallelDelayExecutor) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.starts)
}

type cancelAwareBlockingExecutor struct {
	started  chan string
	canceled chan string
	delay    time.Duration
}

func (e *cancelAwareBlockingExecutor) Execute(ctx context.Context, node dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	runID := inv.Run.ID
	select {
	case e.started <- runID:
	default:
	}
	select {
	case <-time.After(e.delay):
		return dag.ExecutorResult{Outputs: map[string]string{"status": "success"}}, nil
	case <-ctx.Done():
		select {
		case e.canceled <- runID:
		default:
		}
		return dag.ExecutorResult{}, ctx.Err()
	}
}

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
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(map[string]dag.Executor{
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
		if returnStringField(t, result.Return, "outcome") != "1" {
			t.Fatalf("%s outcome = %q, want 1", runID, returnStringField(t, result.Return, "outcome"))
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
				CorrelationKey: "concurrent-signal:judge",
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
	if returnStringField(t, result.Return, "outcome") != "1" {
		t.Fatalf("outcome = %q, want 1", returnStringField(t, result.Return, "outcome"))
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
				CorrelationKey: "cancel-signal-race:judge",
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

// Enqueue contention should leave a pending breadcrumb instead of dropping the run.
func TestDurableEnqueueRaceWithBusyWorkerIsNotLost(t *testing.T) {
	tmpDir := t.TempDir()
	blocking := newDurableReleaseExecutor(map[string]string{"status": "success", "outcome": "1"})
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(map[string]dag.Executor{
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

	// Another worker keeps enqueueing while this one is still busy.
	for i := 0; i < 8; i++ {
		manager.enqueue(runID)
	}

	// A failed claim should still set pending.
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

	// Release the holder and let the pending re-enqueue drain.
	close(blocking.release)
	result := waitForDurableStatus(t, manager, runID, RunStatusCompleted)
	if returnStringField(t, result.Return, "outcome") != "1" {
		t.Fatalf("outcome = %q, want 1", returnStringField(t, result.Return, "outcome"))
	}
}

// A burst of duplicate enqueues should stay bounded.
func TestDurableEnqueueBurstStaysBounded(t *testing.T) {
	tmpDir := t.TempDir()
	blocking := newDurableReleaseExecutor(map[string]string{"status": "success", "outcome": "1"})
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(map[string]dag.Executor{
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

	// Let startup noise settle before sampling.
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

	// pending should be set while the worker is still busy.
	manager.mu.Lock()
	pendingSet := manager.pending[runID]
	manager.mu.Unlock()
	if !pendingSet {
		t.Fatal("pending flag was not set during enqueue burst — dedup did not exercise the processing path")
	}

	close(blocking.release)
	waitForDurableStatus(t, manager, runID, RunStatusCompleted)
}

// Three ready siblings should dispatch in parallel.
func TestDurableParallelBranchesRunConcurrently(t *testing.T) {
	tmpDir := t.TempDir()
	const delay = 200 * time.Millisecond

	root := &durableStaticExecutor{outputs: map[string]string{"status": "success"}}
	siblings := &parallelDelayExecutor{delay: delay}

	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"root":    root,
		"sibling": siblings,
	}), DurableRunManagerConfig{
		MaxWorkers:   1,
		MaxQueueSize: 4,
		PollInterval: 5 * time.Millisecond,
	})
	defer manager.Close()

	bp := mustMarshalBlueprint(dag.Blueprint{
		ID:      "fanout",
		Version: 1,
		Nodes: []dag.NodeDef{
			{ID: "root", Type: "root", Config: map[string]interface{}{}},
			{ID: "a", Type: "sibling", Config: map[string]interface{}{}},
			{ID: "b", Type: "sibling", Config: map[string]interface{}{}},
			{ID: "c", Type: "sibling", Config: map[string]interface{}{}},
		},
		Edges: []dag.EdgeDef{
			{From: "root", To: "a"},
			{From: "root", To: "b"},
			{From: "root", To: "c"},
		},
	})

	wallStart := time.Now()
	if _, err := manager.Submit(RunRequest{RunID: "fanout-1", AppID: 9001, BlueprintJSON: bp}); err != nil {
		t.Fatal(err)
	}
	result := waitForDurableTerminal(t, manager, "fanout-1")
	wall := time.Since(wallStart)

	if result.Status != RunStatusCompleted {
		t.Fatalf("status = %q, want %q", result.Status, RunStatusCompleted)
	}
	if siblings.count() != 3 {
		t.Fatalf("siblings executed %d times, want 3", siblings.count())
	}

	// Serial dispatch would be ~3*delay; parallel should stay near delay.
	if wall > 2*delay {
		t.Fatalf("wall clock = %v, want <= %v (parallel branches regressed to serial dispatch)", wall, 2*delay)
	}

	// The siblings should start in a tight window.
	if spread := siblings.startSpread(); spread > 50*time.Millisecond {
		t.Fatalf("sibling start spread = %v, want <= 50ms (siblings not dispatched concurrently)", spread)
	}
}

// Every sibling in a parallel batch should leave a trace entry.
func TestDurableParallelBranchesAllTracesRecorded(t *testing.T) {
	tmpDir := t.TempDir()
	root := &durableStaticExecutor{outputs: map[string]string{"status": "success"}}
	siblings := &durableStaticExecutor{outputs: map[string]string{"status": "success"}}

	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"root":    root,
		"sibling": siblings,
	}), DurableRunManagerConfig{
		MaxWorkers:   1,
		MaxQueueSize: 4,
		PollInterval: 5 * time.Millisecond,
	})
	defer manager.Close()

	bp := mustMarshalBlueprint(dag.Blueprint{
		ID:      "fanout-traces",
		Version: 1,
		Nodes: []dag.NodeDef{
			{ID: "root", Type: "root", Config: map[string]interface{}{}},
			{ID: "a", Type: "sibling", Config: map[string]interface{}{}},
			{ID: "b", Type: "sibling", Config: map[string]interface{}{}},
			{ID: "c", Type: "sibling", Config: map[string]interface{}{}},
		},
		Edges: []dag.EdgeDef{
			{From: "root", To: "a"},
			{From: "root", To: "b"},
			{From: "root", To: "c"},
		},
	})

	if _, err := manager.Submit(RunRequest{RunID: "fanout-traces", AppID: 9002, BlueprintJSON: bp}); err != nil {
		t.Fatal(err)
	}
	result := waitForDurableTerminal(t, manager, "fanout-traces")
	if result.Status != RunStatusCompleted {
		t.Fatalf("status = %q, want %q", result.Status, RunStatusCompleted)
	}
	if siblings.Count() != 3 {
		t.Fatalf("siblings executed %d times, want 3", siblings.Count())
	}
	for _, id := range []string{"root", "a", "b", "c"} {
		if n := countDurableNodeTraces(result.RunState, id); n != 1 {
			t.Fatalf("node %q traces = %d, want 1", id, n)
		}
		state, ok := result.RunState.NodeStates[id]
		if !ok {
			t.Fatalf("node %q missing from NodeStates", id)
		}
		if state.Status != "completed" {
			t.Fatalf("node %q status = %q, want completed", id, state.Status)
		}
	}

	// Exactly one NodeStarted event per node: the batch should not emit
	// duplicate starts if persistWithRetry re-applies on stale.
	events, err := manager.store.loadEvents("fanout-traces")
	if err != nil {
		t.Fatal(err)
	}
	if got := countDurableEvents(events, "NodeStarted"); got != 4 {
		t.Fatalf("NodeStarted events = %d, want 4", got)
	}
	if got := countDurableEvents(events, "NodeCompleted"); got != 4 {
		t.Fatalf("NodeCompleted events = %d, want 4", got)
	}
}

func TestDurableParallelEarlyReturnCancelsSiblingWork(t *testing.T) {
	tmpDir := t.TempDir()
	root := &durableStaticExecutor{outputs: map[string]string{"status": "success"}}
	slow := &cancelAwareBlockingExecutor{
		started:  make(chan string, 1),
		canceled: make(chan string, 1),
		delay:    5 * time.Second,
	}

	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"root": root,
		"slow": slow,
	}), DurableRunManagerConfig{
		MaxWorkers:   1,
		MaxQueueSize: 4,
		PollInterval: 5 * time.Millisecond,
	})
	defer manager.Close()

	bp := mustMarshalBlueprint(dag.Blueprint{
		ID:      "parallel-return-cancel",
		Version: 1,
		Nodes: []dag.NodeDef{
			{ID: "root", Type: "root", Config: map[string]interface{}{}},
			{ID: "done", Type: "return", Config: map[string]interface{}{
				"value": map[string]interface{}{
					"status":  "success",
					"outcome": "1",
				},
			}},
			{ID: "slow", Type: "slow", Config: map[string]interface{}{}},
		},
		Edges: []dag.EdgeDef{
			{From: "root", To: "done"},
			{From: "root", To: "slow"},
		},
	})

	start := time.Now()
	if _, err := manager.Submit(RunRequest{RunID: "parallel-return-cancel", AppID: 9003, BlueprintJSON: bp}); err != nil {
		t.Fatal(err)
	}
	result := waitForDurableTerminal(t, manager, "parallel-return-cancel")
	if result.Status != RunStatusCompleted {
		t.Fatalf("status = %q, want %q", result.Status, RunStatusCompleted)
	}
	if got := returnStringField(t, result.Return, "outcome"); got != "1" {
		t.Fatalf("outcome = %q, want 1", got)
	}
	if wall := time.Since(start); wall > time.Second {
		t.Fatalf("wall clock = %v, want <= 1s (early return waited for slow sibling)", wall)
	}
	if state := result.RunState.NodeStates["slow"].Status; state != "skipped" {
		t.Fatalf("slow node status = %q, want skipped", state)
	}
	select {
	case runID := <-slow.canceled:
		if runID != "parallel-return-cancel" {
			t.Fatalf("canceled run = %q, want parallel-return-cancel", runID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for slow sibling cancellation")
	}
}
