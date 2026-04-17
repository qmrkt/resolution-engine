package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qmrkt/resolution-engine/dag"
	"github.com/qmrkt/resolution-engine/executors"
)

type durableStaticExecutor struct {
	mu      sync.Mutex
	outputs map[string]string
	count   int
}

func (e *durableStaticExecutor) Execute(ctx context.Context, node dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	e.mu.Lock()
	e.count++
	outputs := cloneStringMap(e.outputs)
	e.mu.Unlock()
	return dag.ExecutorResult{Outputs: outputs}, nil
}

func (e *durableStaticExecutor) Count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.count
}

type durableSequenceExecutor struct {
	mu       sync.Mutex
	outputs  []map[string]string
	count    int
	lastRuns []string
}

func (e *durableSequenceExecutor) Execute(ctx context.Context, node dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	runID := inv.Run.ID
	e.lastRuns = append(e.lastRuns, runID)
	index := e.count
	if index >= len(e.outputs) {
		index = len(e.outputs) - 1
	}
	e.count++
	return dag.ExecutorResult{Outputs: cloneStringMap(e.outputs[index])}, nil
}

func (e *durableSequenceExecutor) Count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.count
}

type durableErrorExecutor struct {
	err error
}

func (e *durableErrorExecutor) Execute(ctx context.Context, node dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	if e.err == nil {
		e.err = errors.New("boom")
	}
	return dag.ExecutorResult{}, e.err
}

type durableBlockingExecutor struct {
	started chan struct{}
}

func (e *durableBlockingExecutor) Execute(ctx context.Context, node dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	select {
	case e.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return dag.ExecutorResult{}, ctx.Err()
}

type durableReleaseExecutor struct {
	started  chan string
	release  chan struct{}
	outputs  map[string]string
	mu       sync.Mutex
	attempts map[string]int
}

func newDurableReleaseExecutor(outputs map[string]string) *durableReleaseExecutor {
	return &durableReleaseExecutor{
		started:  make(chan string, 16),
		release:  make(chan struct{}),
		outputs:  outputs,
		attempts: make(map[string]int),
	}
}

func (e *durableReleaseExecutor) Execute(ctx context.Context, node dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	runID := inv.Run.ID
	e.mu.Lock()
	e.attempts[runID]++
	e.mu.Unlock()
	select {
	case e.started <- runID:
	default:
	}
	select {
	case <-e.release:
		return dag.ExecutorResult{Outputs: cloneStringMap(e.outputs)}, nil
	case <-ctx.Done():
		return dag.ExecutorResult{}, ctx.Err()
	}
}

func (e *durableReleaseExecutor) Attempts(runID string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.attempts[runID]
}

func newDurableTestManager(t *testing.T, storeDir string, engine *dag.Engine, maxWorkers int) *DurableRunManager {
	t.Helper()
	return newDurableTestManagerWithConfig(t, storeDir, engine, DurableRunManagerConfig{
		MaxWorkers:   maxWorkers,
		PollInterval: 10 * time.Millisecond,
	})
}

func newDurableTestManagerWithConfig(t *testing.T, storeDir string, engine *dag.Engine, cfg DurableRunManagerConfig) *DurableRunManager {
	t.Helper()
	runner := &Runner{
		engine:  engine,
		dataDir: filepath.Join(storeDir, "evidence"),
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 10 * time.Millisecond
	}
	manager, err := NewDurableRunManager(runner, filepath.Join(storeDir, "store"), nil, "", cfg)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func durableTestEngine(execs map[string]dag.Executor) *dag.Engine {
	engine := dag.NewEngine(nil)
	for nodeType, exec := range execs {
		engine.RegisterExecutor(nodeType, exec)
	}
	if _, ok := execs["return"]; !ok {
		engine.RegisterExecutor("return", executors.NewReturnExecutor())
	}
	// Register the suspension-capable executors so the generic Suspend
	// path can dispatch them like any other executor. Tests may override
	// these by passing their own executor for the same node type.
	if _, ok := execs["wait"]; !ok {
		engine.RegisterExecutor("wait", executors.NewWaitExecutor())
	}
	if _, ok := execs["await_signal"]; !ok {
		engine.RegisterExecutor("await_signal", executors.NewAwaitSignalExecutor())
	}
	return engine
}

func waitForDurableStatus(t *testing.T, manager *DurableRunManager, runID string, status RunStatus) RunResult {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var last RunResult
	found := false
	for time.Now().Before(deadline) {
		result, ok := manager.Get(runID)
		if ok {
			found = true
			last = result
			if result.Status == status {
				return result
			}
			if isTerminalStatus(result.Status) && !isTerminalStatus(status) {
				t.Fatalf("run %q reached terminal %q while waiting for %q", runID, result.Status, status)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !found {
		t.Fatalf("run %q never appeared", runID)
	}
	t.Fatalf("run %q did not reach %q; last status %q result %+v", runID, status, last.Status, last)
	return RunResult{}
}

func waitForEventType(t *testing.T, manager *DurableRunManager, runID string, eventType string) []durableEvent {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		events, err := manager.store.loadEvents(runID)
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range events {
			if event.Type == eventType {
				return events
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for event %q on run %q", eventType, runID)
	return nil
}

func returnSuccessNode(id string, outcomeKey string) dag.NodeDef {
	return dag.NodeDef{
		ID:   id,
		Type: "return",
		Config: map[string]interface{}{
			"value": map[string]interface{}{
				"status":  "success",
				"outcome": "{{" + outcomeKey + "}}",
			},
		},
	}
}

func simpleSubmitBlueprint(stepType string) []byte {
	return mustMarshalBlueprint(dag.Blueprint{
		ID:      "simple",
		Version: 1,
		Nodes: []dag.NodeDef{
			{ID: "step", Type: stepType, Config: map[string]interface{}{}},
			returnSuccessNode("submit", "results.step.outcome"),
		},
		Edges: []dag.EdgeDef{{From: "step", To: "submit"}},
	})
}

func joinSubmitBlueprint(leftType, rightType string) []byte {
	return mustMarshalBlueprint(dag.Blueprint{
		ID:      "join-submit",
		Version: 1,
		Nodes: []dag.NodeDef{
			{ID: "left", Type: leftType, Config: map[string]interface{}{}},
			{ID: "right", Type: rightType, Config: map[string]interface{}{}},
			returnSuccessNode("submit", "results.right.outcome"),
		},
		Edges: []dag.EdgeDef{
			{From: "left", To: "submit"},
			{From: "right", To: "submit"},
		},
	})
}

func waitSubmitBlueprint() []byte {
	return mustMarshalBlueprint(dag.Blueprint{
		ID:      "wait-submit",
		Version: 1,
		Nodes: []dag.NodeDef{
			{ID: "wait", Type: "wait", Config: map[string]interface{}{"duration_seconds": 1, "max_inline_seconds": -1}},
			{ID: "step", Type: "outcome", Config: map[string]interface{}{}},
			returnSuccessNode("submit", "results.step.outcome"),
		},
		Edges: []dag.EdgeDef{
			{From: "wait", To: "step"},
			{From: "step", To: "submit"},
		},
	})
}

func awaitSignalBlueprint() []byte {
	return mustMarshalBlueprint(dag.Blueprint{
		ID:      "await-signal-submit",
		Version: 1,
		Nodes: []dag.NodeDef{
			{ID: "judge", Type: "await_signal", Config: map[string]interface{}{
				"signal_type":      "human_judgment.responded",
				"required_payload": []string{"outcome"},
				"default_outputs":  map[string]string{"status": "responded"},
				"timeout_seconds":  60,
			}},
			returnSuccessNode("submit", "results.judge.outcome"),
		},
		Edges: []dag.EdgeDef{{From: "judge", To: "submit"}},
	})
}

func deferTerminalBlueprint() []byte {
	return mustMarshalBlueprint(dag.Blueprint{
		ID:      "defer-terminal",
		Version: 1,
		Nodes: []dag.NodeDef{
			{ID: "defer", Type: "return", Config: map[string]interface{}{
				"value": map[string]interface{}{
					"status": "deferred",
					"reason": "need new chain data",
				},
			}},
		},
	})
}

func loopWithWaitBlueprint() []byte {
	return mustMarshalBlueprint(dag.Blueprint{
		ID:      "loop-wait",
		Version: 1,
		Nodes: []dag.NodeDef{
			{ID: "fetch", Type: "fetch", Config: map[string]interface{}{}},
			{ID: "wait", Type: "wait", Config: map[string]interface{}{"duration_seconds": 1, "max_inline_seconds": -1}},
			returnSuccessNode("submit", "results.fetch.outcome"),
		},
		Edges: []dag.EdgeDef{
			{From: "fetch", To: "wait", Condition: "results.fetch.status != 'success'"},
			{From: "wait", To: "fetch", MaxTraversals: 1},
			{From: "fetch", To: "submit", Condition: "results.fetch.status == 'success'"},
		},
	})
}

func mustMarshalBlueprint(bp dag.Blueprint) []byte {
	data, err := jsonMarshal(bp)
	if err != nil {
		panic(err)
	}
	return data
}

func jsonMarshal(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func TestDurableRunCompletesAcrossRestartAfterInterruptedWorker(t *testing.T) {
	tmpDir := t.TempDir()
	blocking := &durableBlockingExecutor{started: make(chan struct{}, 1)}
	manager := newDurableTestManager(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"outcome": blocking,
	}), 1)

	if _, err := manager.Submit(RunRequest{RunID: "restart-run", AppID: 1001, BlueprintJSON: simpleSubmitBlueprint("outcome")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocking.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first worker attempt")
	}
	manager.Close()

	success := &durableStaticExecutor{outputs: map[string]string{"status": "success", "outcome": "1"}}
	manager = newDurableTestManager(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"outcome": success,
	}), 1)
	defer manager.Close()

	result := waitForDurableStatus(t, manager, "restart-run", RunStatusCompleted)
	if returnStringField(t, result.Return, "outcome") != "1" || result.RunState == nil || result.RunState.ID != "restart-run" {
		t.Fatalf("unexpected completed result: %+v", result)
	}

	events, err := manager.store.loadEvents("restart-run")
	if err != nil {
		t.Fatal(err)
	}
	starts := 0
	for _, event := range events {
		if event.Type == "NodeStarted" {
			starts++
		}
	}
	if starts < 2 {
		t.Fatalf("expected event log to detect retry with at least two NodeStarted events, got %d events=%+v", starts, events)
	}
}

func TestDurableWaitSuspendsWithoutConsumingWorker(t *testing.T) {
	tmpDir := t.TempDir()
	outcome := &durableStaticExecutor{outputs: map[string]string{"status": "success", "outcome": "1"}}
	manager := newDurableTestManager(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"outcome": outcome,
	}), 1)
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "wait-run", AppID: 1002, BlueprintJSON: waitSubmitBlueprint()}); err != nil {
		t.Fatal(err)
	}
	waiting := waitForDurableStatus(t, manager, "wait-run", RunStatusWaiting)
	if waiting.RunState.NodeStates["wait"].Status != "waiting" {
		t.Fatalf("wait node status = %q, want waiting", waiting.RunState.NodeStates["wait"].Status)
	}

	if _, err := manager.Submit(RunRequest{RunID: "simple-while-waiting", AppID: 1003, BlueprintJSON: simpleSubmitBlueprint("outcome")}); err != nil {
		t.Fatal(err)
	}
	quick := waitForDurableStatus(t, manager, "simple-while-waiting", RunStatusCompleted)
	if returnStringField(t, quick.Return, "outcome") != "1" {
		t.Fatalf("quick run outcome = %q, want 1", returnStringField(t, quick.Return, "outcome"))
	}

	delayed := waitForDurableStatus(t, manager, "wait-run", RunStatusCompleted)
	if returnStringField(t, delayed.Return, "outcome") != "1" {
		t.Fatalf("delayed run outcome = %q, want 1", returnStringField(t, delayed.Return, "outcome"))
	}
}

func TestDurableAwaitSignalResumesBySignalAndDedupes(t *testing.T) {
	tmpDir := t.TempDir()
	manager := newDurableTestManager(t, tmpDir, durableTestEngine(nil), 1)
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "human-run", AppID: 1004, BlueprintJSON: awaitSignalBlueprint()}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "human-run", RunStatusWaiting)

	signal := signalRequest{
		IdempotencyKey: "human-signal-1",
		AppID:          1004,
		RunID:          "human-run",
		SignalType:     "human_judgment.responded",
		CorrelationKey: "human-run:judge",
		Payload: map[string]string{
			"outcome": "1",
			"reason":  "clear evidence",
		},
	}
	if _, err := manager.Signal(signal); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Signal(signal); err != nil {
		t.Fatalf("duplicate signal should be idempotent: %v", err)
	}

	result := waitForDurableStatus(t, manager, "human-run", RunStatusCompleted)
	if returnStringField(t, result.Return, "outcome") != "1" {
		t.Fatalf("outcome = %q, want 1", returnStringField(t, result.Return, "outcome"))
	}
	if got := testResultValue(result.RunState, "judge", "reason"); got != "clear evidence" {
		t.Fatalf("judge reason = %q, want clear evidence", got)
	}
}

func TestDurableDeferResolutionCompletesRunAsDeferred(t *testing.T) {
	tmpDir := t.TempDir()
	manager := newDurableTestManager(t, tmpDir, durableTestEngine(nil), 1)
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "defer-run", AppID: 1005, BlueprintJSON: deferTerminalBlueprint()}); err != nil {
		t.Fatal(err)
	}

	result := waitForDurableStatus(t, manager, "defer-run", RunStatusCompleted)
	if returnStringField(t, result.Return, "status") != "deferred" {
		t.Fatalf("status = %q, want deferred", returnStringField(t, result.Return, "status"))
	}
	if returnStringField(t, result.Return, "reason") != "need new chain data" {
		t.Fatalf("reason = %q, want need new chain data", returnStringField(t, result.Return, "reason"))
	}
	if result.RunState == nil || result.RunState.NodeStates["defer"].Status != "completed" {
		t.Fatalf("defer node did not complete: %+v", result.RunState)
	}
}

func TestDurableManagerRejectsDuplicateActiveApp(t *testing.T) {
	tmpDir := t.TempDir()
	manager := newDurableTestManager(t, tmpDir, durableTestEngine(nil), 1)
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "human-dup-a", AppID: 1006, BlueprintJSON: awaitSignalBlueprint()}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "human-dup-a", RunStatusWaiting)

	_, err := manager.Submit(RunRequest{RunID: "human-dup-b", AppID: 1006, BlueprintJSON: awaitSignalBlueprint()})
	var dup *duplicateRunError
	if !errors.As(err, &dup) {
		t.Fatalf("expected duplicateRunError, got %v", err)
	}
	if dup.RunID != "human-dup-a" {
		t.Fatalf("duplicate run id = %q, want human-dup-a", dup.RunID)
	}
}

func TestDurableSingleNodeQueueCapacityReturnsStableOverload(t *testing.T) {
	tmpDir := t.TempDir()
	blocking := newDurableReleaseExecutor(map[string]string{"status": "success", "outcome": "1"})
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"outcome": blocking,
	}), DurableRunManagerConfig{
		MaxWorkers:   1,
		MaxQueueSize: 1,
		PollInterval: 10 * time.Millisecond,
	})
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "busy-run", AppID: 1101, BlueprintJSON: simpleSubmitBlueprint("outcome")}); err != nil {
		t.Fatal(err)
	}
	select {
	case runID := <-blocking.started:
		if runID != "busy-run" {
			t.Fatalf("started run = %q, want busy-run", runID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for busy worker")
	}

	if _, err := manager.Submit(RunRequest{RunID: "queued-run", AppID: 1102, BlueprintJSON: simpleSubmitBlueprint("outcome")}); err != nil {
		t.Fatal(err)
	}
	queued := waitForDurableStatus(t, manager, "queued-run", RunStatusQueued)
	if queued.AppID != 1102 {
		t.Fatalf("queued app_id = %d, want 1102", queued.AppID)
	}

	_, err := manager.Submit(RunRequest{RunID: "overflow-run", AppID: 1103, BlueprintJSON: simpleSubmitBlueprint("outcome")})
	var full *queueFullError
	if !errors.As(err, &full) {
		t.Fatalf("expected queueFullError, got %v", err)
	}
	if full.MaxQueueSize != 1 {
		t.Fatalf("max queue size = %d, want 1", full.MaxQueueSize)
	}
	close(blocking.release)
	waitForDurableStatus(t, manager, "busy-run", RunStatusCompleted)
	waitForDurableStatus(t, manager, "queued-run", RunStatusCompleted)
}

func TestDurableSingleNodeDuplicateLocalEnqueueExecutesOnce(t *testing.T) {
	tmpDir := t.TempDir()
	outcome := &durableStaticExecutor{outputs: map[string]string{"status": "success", "outcome": "1"}}
	manager := newDurableTestManager(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"outcome": outcome,
	}), 1)
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "dupe-enqueue", AppID: 1104, BlueprintJSON: simpleSubmitBlueprint("outcome")}); err != nil {
		t.Fatal(err)
	}
	manager.enqueue("dupe-enqueue")
	manager.enqueue("dupe-enqueue")
	waitForDurableStatus(t, manager, "dupe-enqueue", RunStatusCompleted)
	if outcome.Count() != 1 {
		t.Fatalf("outcome executor count = %d, want 1", outcome.Count())
	}
}

func TestDurableActiveCountIncludesQueuedRunningAndWaiting(t *testing.T) {
	tmpDir := t.TempDir()
	blocking := newDurableReleaseExecutor(map[string]string{"status": "success", "outcome": "1"})
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"outcome": blocking,
	}), DurableRunManagerConfig{
		MaxWorkers:   1,
		MaxQueueSize: 10,
		PollInterval: 10 * time.Millisecond,
	})
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "active-running", AppID: 1105, BlueprintJSON: simpleSubmitBlueprint("outcome")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocking.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for running run")
	}
	if _, err := manager.Submit(RunRequest{RunID: "active-queued", AppID: 1106, BlueprintJSON: simpleSubmitBlueprint("outcome")}); err != nil {
		t.Fatal(err)
	}
	if active := manager.ActiveCount(); active != 2 {
		t.Fatalf("active count with running+queued = %d, want 2", active)
	}
	close(blocking.release)
	waitForDurableStatus(t, manager, "active-running", RunStatusCompleted)
	waitForDurableStatus(t, manager, "active-queued", RunStatusCompleted)

	if _, err := manager.Submit(RunRequest{RunID: "active-waiting", AppID: 1107, BlueprintJSON: awaitSignalBlueprint()}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "active-waiting", RunStatusWaiting)
	if active := manager.ActiveCount(); active != 1 {
		t.Fatalf("active count with waiting run = %d, want 1", active)
	}
}

func TestDurableCheckpointContainsResumeStateAndHistoryEvents(t *testing.T) {
	tmpDir := t.TempDir()
	manager := newDurableTestManager(t, tmpDir, durableTestEngine(nil), 1)
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "checkpoint-human", AppID: 1108, BlueprintJSON: awaitSignalBlueprint()}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "checkpoint-human", RunStatusWaiting)
	record, err := manager.store.loadRun("checkpoint-human")
	if err != nil {
		t.Fatal(err)
	}
	if record.Checkpoint.Run == nil {
		t.Fatal("checkpoint missing run state")
	}
	if record.Checkpoint.Run.Inputs == nil {
		t.Fatal("checkpoint missing inputs")
	}
	if !record.Checkpoint.Activated["judge"] {
		t.Fatalf("checkpoint activated set = %+v, want judge", record.Checkpoint.Activated)
	}
	if record.Checkpoint.IterationCount["judge"] != 1 {
		t.Fatalf("iteration count = %+v, want judge=1", record.Checkpoint.IterationCount)
	}
	waiting, ok := record.Checkpoint.Waiting["judge"]
	if !ok {
		t.Fatalf("waiting nodes = %+v, want judge", record.Checkpoint.Waiting)
	}
	if waiting.CorrelationKey != "checkpoint-human:judge" {
		t.Fatalf("correlation key = %q", waiting.CorrelationKey)
	}

	events := waitForEventType(t, manager, "checkpoint-human", "RunWaiting")
	required := map[string]bool{"RunQueued": false, "RunRunning": false, "NodeStarted": false, "NodeSuspended": false, "RunWaiting": false}
	for _, event := range events {
		if _, ok := required[event.Type]; ok {
			required[event.Type] = true
		}
	}
	for eventType, seen := range required {
		if !seen {
			t.Fatalf("event %s not found in %+v", eventType, events)
		}
	}
}

func TestDurableCorruptRunFileFailsClosedOnStartup(t *testing.T) {
	tmpDir := t.TempDir()
	storeDir := filepath.Join(tmpDir, "store")
	if err := os.MkdirAll(filepath.Join(storeDir, "runs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(storeDir, "runs", "bad.json"), []byte(`{"not valid"`), 0o644); err != nil {
		t.Fatal(err)
	}
	runner := &Runner{engine: durableTestEngine(nil), dataDir: filepath.Join(tmpDir, "evidence")}
	manager, err := NewDurableRunManager(runner, storeDir, nil, "", DurableRunManagerConfig{MaxWorkers: 1, PollInterval: 10 * time.Millisecond})
	if err == nil {
		manager.Close()
		t.Fatal("expected corrupt checkpoint to fail manager startup")
	}
}

func TestDurableEarlySignalIsRejectedConsistently(t *testing.T) {
	tmpDir := t.TempDir()
	manager := newDurableTestManager(t, tmpDir, durableTestEngine(nil), 1)
	defer manager.Close()

	_, err := manager.Signal(signalRequest{
		IdempotencyKey: "early-signal",
		AppID:          1109,
		SignalType:     "human_judgment.responded",
		CorrelationKey: "missing:judge",
		Payload:        map[string]string{"outcome": "1"},
	})
	if err == nil || !strings.Contains(err.Error(), "no matching run") {
		t.Fatalf("expected no matching run error, got %v", err)
	}
}

func TestDurableNonMatchingSignalIsStoredButDoesNotResume(t *testing.T) {
	tmpDir := t.TempDir()
	manager := newDurableTestManager(t, tmpDir, durableTestEngine(nil), 1)
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "nonmatching-signal", AppID: 1110, BlueprintJSON: awaitSignalBlueprint()}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "nonmatching-signal", RunStatusWaiting)
	if _, err := manager.Signal(signalRequest{
		IdempotencyKey: "wrong-signal",
		AppID:          1110,
		RunID:          "nonmatching-signal",
		SignalType:     "human_judgment.responded",
		CorrelationKey: "nonmatching-signal:other-node",
		Payload:        map[string]string{"outcome": "1"},
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	result, ok := manager.Get("nonmatching-signal")
	if !ok {
		t.Fatal("expected run to exist")
	}
	if result.Status != RunStatusWaiting {
		t.Fatalf("status after nonmatching signal = %q, want waiting", result.Status)
	}
	record, err := manager.store.loadRun("nonmatching-signal")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := record.Signals["wrong-signal"]; !ok {
		t.Fatalf("signal was not durably stored: %+v", record.Signals)
	}
}

func TestDurableAwaitSignalTimeoutResumesThroughTimeoutPath(t *testing.T) {
	tmpDir := t.TempDir()
	manager := newDurableTestManager(t, tmpDir, durableTestEngine(nil), 1)
	defer manager.Close()

	bp := mustMarshalBlueprint(dag.Blueprint{
		ID:      "human-timeout",
		Version: 1,
		Nodes: []dag.NodeDef{
			{ID: "judge", Type: "await_signal", Config: map[string]interface{}{
				"signal_type":      "human_judgment.responded",
				"required_payload": []string{"outcome"},
				"default_outputs":  map[string]string{"status": "responded"},
				"timeout_seconds":  1,
			}},
			{ID: "cancel", Type: "return", Config: map[string]interface{}{
				"value": map[string]interface{}{
					"status": "failed",
					"reason": "human timeout",
				},
			}},
		},
		Edges: []dag.EdgeDef{{From: "judge", To: "cancel", Condition: "results.judge.status == 'timeout'"}},
	})
	if _, err := manager.Submit(RunRequest{RunID: "human-timeout", AppID: 1111, BlueprintJSON: bp}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "human-timeout", RunStatusWaiting)
	result := waitForDurableStatus(t, manager, "human-timeout", RunStatusCompleted)
	if returnStringField(t, result.Return, "status") != "failed" {
		t.Fatalf("status = %q, want failed result=%+v", returnStringField(t, result.Return, "status"), result)
	}
	if got := testResultValue(result.RunState, "judge", "status"); got != "timeout" {
		t.Fatalf("judge.status = %q, want timeout", got)
	}
}

func TestDurableCancellationIgnoresOutstandingTimer(t *testing.T) {
	tmpDir := t.TempDir()
	outcome := &durableStaticExecutor{outputs: map[string]string{"status": "success", "outcome": "1"}}
	manager := newDurableTestManager(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"outcome": outcome,
	}), 1)
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "cancel-wait", AppID: 1112, BlueprintJSON: waitSubmitBlueprint()}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "cancel-wait", RunStatusWaiting)
	if _, ok := manager.Cancel("cancel-wait"); !ok {
		t.Fatal("cancel did not find run")
	}
	waitForDurableStatus(t, manager, "cancel-wait", RunStatusCancelled)
	time.Sleep(1200 * time.Millisecond)
	result, ok := manager.Get("cancel-wait")
	if !ok {
		t.Fatal("expected cancelled run to remain readable")
	}
	if result.Status != RunStatusCancelled {
		t.Fatalf("status after timer matured = %q, want cancelled", result.Status)
	}
	if outcome.Count() != 0 {
		t.Fatalf("downstream executor ran after cancellation count=%d", outcome.Count())
	}
}

func TestDurableBeginShutdownRejectsNewMutations(t *testing.T) {
	tmpDir := t.TempDir()
	manager := newDurableTestManager(t, tmpDir, durableTestEngine(nil), 1)
	defer manager.Close()

	manager.BeginShutdown()

	if _, err := manager.Submit(RunRequest{
		RunID:         "shutdown-submit",
		AppID:         1113,
		BlueprintJSON: awaitSignalBlueprint(),
	}); !errors.Is(err, errDurableManagerShuttingDown) {
		t.Fatalf("submit error = %v, want %v", err, errDurableManagerShuttingDown)
	}

	if _, err := manager.Signal(signalRequest{
		IdempotencyKey: "shutdown-signal",
		AppID:          1113,
		RunID:          "shutdown-submit",
		SignalType:     "human_judgment.responded",
		CorrelationKey: "shutdown-submit:judge",
		Payload:        map[string]string{"outcome": "1"},
	}); !errors.Is(err, errDurableManagerShuttingDown) {
		t.Fatalf("signal error = %v, want %v", err, errDurableManagerShuttingDown)
	}
}

func TestDurableBeginShutdownCancelsInFlightWorker(t *testing.T) {
	tmpDir := t.TempDir()
	blocking := &durableBlockingExecutor{started: make(chan struct{}, 1)}
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"blocking": blocking,
	}), DurableRunManagerConfig{
		MaxWorkers:   1,
		PollInterval: 10 * time.Millisecond,
	})
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{
		RunID:         "shutdown-cancel",
		AppID:         1114,
		BlueprintJSON: simpleSubmitBlueprint("blocking"),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocking.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for blocking executor to start")
	}

	manager.BeginShutdown()
	waitForDurableNotProcessing(t, manager, "shutdown-cancel")

	record, err := manager.store.loadRun("shutdown-cancel")
	if err != nil {
		t.Fatal(err)
	}
	if state := record.Checkpoint.Run.NodeStates["step"].Status; state != "running" {
		t.Fatalf("node state after shutdown = %q, want running checkpoint for recovery", state)
	}
}

func TestDurableCallbackOutboxRetriesUntilSuccess(t *testing.T) {
	tmpDir := t.TempDir()
	var attempts atomic.Int32
	callback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		if attempt == 1 {
			http.Error(w, "not yet", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer callback.Close()

	outcome := &durableStaticExecutor{outputs: map[string]string{"status": "success", "outcome": "1"}}
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"outcome": outcome,
	}), DurableRunManagerConfig{MaxWorkers: 1, PollInterval: 10 * time.Millisecond})
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "callback-retry", AppID: 1113, BlueprintJSON: simpleSubmitBlueprint("outcome"), CallbackURL: callback.URL}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "callback-retry", RunStatusCompleted)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		record, err := manager.store.loadRun("callback-retry")
		if err != nil {
			t.Fatal(err)
		}
		if record.CallbackDelivered {
			if attempts.Load() < 2 {
				t.Fatalf("callback delivered after %d attempts, want retry", attempts.Load())
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("callback was not delivered after retry; attempts=%d", attempts.Load())
}

func TestDurableSignalCanResumeWhileOnlyWorkerIsBusy(t *testing.T) {
	tmpDir := t.TempDir()
	blocking := newDurableReleaseExecutor(map[string]string{"status": "success", "outcome": "1"})
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"outcome": blocking,
	}), DurableRunManagerConfig{MaxWorkers: 1, MaxQueueSize: 10, PollInterval: 10 * time.Millisecond})
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "waiting-human", AppID: 1114, BlueprintJSON: awaitSignalBlueprint()}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "waiting-human", RunStatusWaiting)
	if _, err := manager.Submit(RunRequest{RunID: "busy-signal-worker", AppID: 1115, BlueprintJSON: simpleSubmitBlueprint("outcome")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocking.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for busy worker")
	}

	if _, err := manager.Signal(signalRequest{
		IdempotencyKey: "resume-while-busy",
		AppID:          1114,
		RunID:          "waiting-human",
		SignalType:     "human_judgment.responded",
		CorrelationKey: "waiting-human:judge",
		Payload:        map[string]string{"outcome": "1"},
	}); err != nil {
		t.Fatal(err)
	}
	queued := waitForDurableStatus(t, manager, "waiting-human", RunStatusQueued)
	if queued.RunState.NodeStates["judge"].Status != "completed" {
		t.Fatalf("judge node status = %q, want completed", queued.RunState.NodeStates["judge"].Status)
	}
	close(blocking.release)
	waitForDurableStatus(t, manager, "busy-signal-worker", RunStatusCompleted)
	waitForDurableStatus(t, manager, "waiting-human", RunStatusCompleted)
}

func TestDurableExecutorFailureFailsRunWithEvents(t *testing.T) {
	tmpDir := t.TempDir()
	manager := newDurableTestManager(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"failing": &durableErrorExecutor{err: errors.New("upstream exploded")},
	}), 1)
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "failure-run", AppID: 1116, BlueprintJSON: simpleSubmitBlueprint("failing")}); err != nil {
		t.Fatal(err)
	}
	result := waitForDurableStatus(t, manager, "failure-run", RunStatusFailed)
	if result.RunState.NodeStates["step"].Status != "failed" {
		t.Fatalf("step status = %q, want failed", result.RunState.NodeStates["step"].Status)
	}
	events, err := manager.store.loadEvents("failure-run")
	if err != nil {
		t.Fatal(err)
	}
	seenFailed := false
	for _, event := range events {
		if event.Type == "NodeFailed" {
			seenFailed = true
		}
	}
	if !seenFailed {
		t.Fatalf("NodeFailed event not found: %+v", events)
	}
}

func TestDurablePartialFrontierRecoveryDoesNotRerunCompletedNode(t *testing.T) {
	tmpDir := t.TempDir()
	left := &durableStaticExecutor{outputs: map[string]string{"status": "success"}}
	rightBlock := newDurableReleaseExecutor(map[string]string{"status": "success", "outcome": "1"})
	manager := newDurableTestManager(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"left_exec":  left,
		"right_exec": rightBlock,
	}), 1)

	if _, err := manager.Submit(RunRequest{RunID: "partial-frontier", AppID: 1117, BlueprintJSON: joinSubmitBlueprint("left_exec", "right_exec")}); err != nil {
		t.Fatal(err)
	}
	select {
	case runID := <-rightBlock.started:
		if runID != "partial-frontier" {
			t.Fatalf("started run = %q, want partial-frontier", runID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for right node")
	}
	if left.Count() != 1 {
		t.Fatalf("left count before restart = %d, want 1", left.Count())
	}
	manager.Close()

	rightSuccess := &durableStaticExecutor{outputs: map[string]string{"status": "success", "outcome": "1"}}
	manager = newDurableTestManager(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"left_exec":  left,
		"right_exec": rightSuccess,
	}), 1)
	defer manager.Close()

	result := waitForDurableStatus(t, manager, "partial-frontier", RunStatusCompleted)
	if returnStringField(t, result.Return, "outcome") != "1" {
		t.Fatalf("outcome = %q, want 1", returnStringField(t, result.Return, "outcome"))
	}
	if left.Count() != 1 {
		t.Fatalf("completed left node reran after restart; count=%d", left.Count())
	}
}

func TestDurableBackEdgeHistorySurvivesSuspendResume(t *testing.T) {
	tmpDir := t.TempDir()
	fetch := &durableSequenceExecutor{outputs: []map[string]string{
		{"status": "retry", "outcome": ""},
		{"status": "success", "outcome": "1"},
	}}
	manager := newDurableTestManager(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"fetch": fetch,
	}), 1)
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "loop-history", AppID: 1118, BlueprintJSON: loopWithWaitBlueprint()}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "loop-history", RunStatusWaiting)
	result := waitForDurableStatus(t, manager, "loop-history", RunStatusCompleted)
	if returnStringField(t, result.Return, "outcome") != "1" {
		t.Fatalf("outcome = %q, want 1", returnStringField(t, result.Return, "outcome"))
	}
	if fetch.Count() != 2 {
		t.Fatalf("fetch count = %d, want 2", fetch.Count())
	}
	history := testResultHistoryJSON(result.RunState, "fetch")
	if !strings.Contains(history, `"status":"retry"`) {
		t.Fatalf("fetch history missing retry snapshot: %q", history)
	}
}

func TestDurableDueTimerIsIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	outcome := &durableStaticExecutor{outputs: map[string]string{"status": "success", "outcome": "1"}}
	manager := newDurableTestManager(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"outcome": outcome,
	}), 1)
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "timer-idempotent", AppID: 1119, BlueprintJSON: waitSubmitBlueprint()}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "timer-idempotent", RunStatusWaiting)
	time.Sleep(1100 * time.Millisecond)
	if err := manager.wakeDueRuns(); err != nil {
		t.Fatal(err)
	}
	if err := manager.wakeDueRuns(); err != nil {
		t.Fatal(err)
	}
	result := waitForDurableStatus(t, manager, "timer-idempotent", RunStatusCompleted)
	if returnStringField(t, result.Return, "outcome") != "1" {
		t.Fatalf("outcome = %q, want 1", returnStringField(t, result.Return, "outcome"))
	}
	if outcome.Count() != 1 {
		t.Fatalf("downstream executor count = %d, want 1", outcome.Count())
	}
}

func TestDurableImmediateWaitDoesNotSuspend(t *testing.T) {
	tmpDir := t.TempDir()
	outcome := &durableStaticExecutor{outputs: map[string]string{"status": "success", "outcome": "1"}}
	manager := newDurableTestManager(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"outcome": outcome,
	}), 1)
	defer manager.Close()

	bp := mustMarshalBlueprint(dag.Blueprint{
		ID:      "immediate-wait",
		Version: 1,
		Nodes: []dag.NodeDef{
			{ID: "wait", Type: "wait", Config: map[string]interface{}{"duration_seconds": 0}},
			{ID: "step", Type: "outcome", Config: map[string]interface{}{}},
			returnSuccessNode("submit", "results.step.outcome"),
		},
		Edges: []dag.EdgeDef{{From: "wait", To: "step"}, {From: "step", To: "submit"}},
	})
	if _, err := manager.Submit(RunRequest{RunID: "immediate-wait", AppID: 1120, BlueprintJSON: bp}); err != nil {
		t.Fatal(err)
	}
	result := waitForDurableStatus(t, manager, "immediate-wait", RunStatusCompleted)
	if result.RunState.NodeStates["wait"].Status != "completed" {
		t.Fatalf("wait status = %q, want completed", result.RunState.NodeStates["wait"].Status)
	}
	if len(testResultValue(result.RunState, "wait", "ready_at")) != 0 {
		t.Fatalf("unexpected ready_at for immediate wait: %q", testResultValue(result.RunState, "wait", "ready_at"))
	}
}

func TestDurableSignalCanCorrelateByAppIDWithoutRunID(t *testing.T) {
	tmpDir := t.TempDir()
	manager := newDurableTestManager(t, tmpDir, durableTestEngine(nil), 1)
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "app-signal-run", AppID: 1121, BlueprintJSON: awaitSignalBlueprint()}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "app-signal-run", RunStatusWaiting)
	if _, err := manager.Signal(signalRequest{
		IdempotencyKey: "app-only-signal",
		AppID:          1121,
		SignalType:     "human_judgment.responded",
		CorrelationKey: "app-signal-run:judge",
		Payload:        map[string]string{"outcome": "1"},
	}); err != nil {
		t.Fatal(err)
	}
	result := waitForDurableStatus(t, manager, "app-signal-run", RunStatusCompleted)
	if returnStringField(t, result.Return, "outcome") != "1" {
		t.Fatalf("outcome = %q, want 1", returnStringField(t, result.Return, "outcome"))
	}
}

func TestDurableSignalValidationHappensBeforeCheckpointMutation(t *testing.T) {
	tmpDir := t.TempDir()
	manager := newDurableTestManager(t, tmpDir, durableTestEngine(nil), 1)
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "invalid-signal", AppID: 1122, BlueprintJSON: awaitSignalBlueprint()}); err != nil {
		t.Fatal(err)
	}
	before := waitForDurableStatus(t, manager, "invalid-signal", RunStatusWaiting)
	if _, err := manager.Signal(signalRequest{
		IdempotencyKey: "invalid-signal-1",
		AppID:          1122,
		RunID:          "invalid-signal",
		SignalType:     "human_judgment.responded",
		CorrelationKey: "invalid-signal:judge",
		Payload:        map[string]string{"reason": "missing outcome"},
	}); err == nil {
		t.Fatal("expected signal validation error")
	}
	after, ok := manager.Get("invalid-signal")
	if !ok {
		t.Fatal("expected run to exist")
	}
	if after.Status != RunStatusWaiting {
		t.Fatalf("status after invalid signal = %q, want waiting", after.Status)
	}
	if after.RunState.NodeStates["judge"].Status != before.RunState.NodeStates["judge"].Status {
		t.Fatalf("judge status mutated after invalid signal: before=%q after=%q", before.RunState.NodeStates["judge"].Status, after.RunState.NodeStates["judge"].Status)
	}
	record, err := manager.store.loadRun("invalid-signal")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := record.Signals["invalid-signal-1"]; ok {
		t.Fatalf("invalid signal was persisted: %+v", record.Signals)
	}
}

func TestDurableCallbackOutboxSurvivesManagerRestart(t *testing.T) {
	tmpDir := t.TempDir()
	var allowSuccess atomic.Bool
	var attempts atomic.Int32
	callback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		if !allowSuccess.Load() {
			http.Error(w, "still down", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer callback.Close()

	outcome := &durableStaticExecutor{outputs: map[string]string{"status": "success", "outcome": "1"}}
	engine := durableTestEngine(map[string]dag.Executor{"outcome": outcome})
	manager := newDurableTestManagerWithConfig(t, tmpDir, engine, DurableRunManagerConfig{
		MaxWorkers:   1,
		PollInterval: 10 * time.Millisecond,
	})

	if _, err := manager.Submit(RunRequest{RunID: "callback-restart", AppID: 1124, BlueprintJSON: simpleSubmitBlueprint("outcome"), CallbackURL: callback.URL}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "callback-restart", RunStatusCompleted)
	waitForEventType(t, manager, "callback-restart", "CallbackFailed")
	manager.Close()

	allowSuccess.Store(true)
	manager = newDurableTestManagerWithConfig(t, tmpDir, engine, DurableRunManagerConfig{
		MaxWorkers:   1,
		PollInterval: 10 * time.Millisecond,
	})
	defer manager.Close()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		record, err := manager.store.loadRun("callback-restart")
		if err != nil {
			t.Fatal(err)
		}
		if record.CallbackDelivered {
			if attempts.Load() < 2 {
				t.Fatalf("callback delivered without retry after restart; attempts=%d", attempts.Load())
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("callback not delivered after manager restart; attempts=%d", attempts.Load())
}

func TestDurableMapExecutorCompletesOnce(t *testing.T) {
	tmpDir := t.TempDir()
	step := &durableStaticExecutor{outputs: map[string]string{"status": "success", "processed": "true"}}
	outcome := &durableStaticExecutor{outputs: map[string]string{"status": "success", "outcome": "1"}}
	engine := durableTestEngine(map[string]dag.Executor{
		"step":    step,
		"outcome": outcome,
	})
	engine.RegisterExecutor("map", executors.NewMapExecutor(engine))
	manager := newDurableTestManager(t, tmpDir, engine, 1)
	defer manager.Close()

	bp := mustMarshalBlueprint(dag.Blueprint{
		ID:      "durable-map",
		Version: 1,
		Nodes: []dag.NodeDef{
			{ID: "mapper", Type: "map", Config: map[string]interface{}{
				"items_key":       "inputs.items_json",
				"batch_size":      2,
				"max_concurrency": 1,
				"inline": &dag.Blueprint{
					Nodes: []dag.NodeDef{
						{ID: "step", Type: "step", Config: map[string]interface{}{}},
						{ID: "out", Type: "return", Config: map[string]interface{}{
							"value": map[string]interface{}{
								"status":    "success",
								"processed": "{{results.step.processed}}",
							},
						}},
					},
					Edges: []dag.EdgeDef{{From: "step", To: "out"}},
				},
			}},
			{ID: "outcome", Type: "outcome", Config: map[string]interface{}{}},
			returnSuccessNode("submit", "results.outcome.outcome"),
		},
		Edges: []dag.EdgeDef{
			{From: "mapper", To: "outcome", Condition: "results.mapper.status == 'success'"},
			{From: "outcome", To: "submit"},
		},
	})

	if _, err := manager.Submit(RunRequest{
		RunID:         "durable-map",
		AppID:         1125,
		BlueprintJSON: bp,
		Inputs:        map[string]string{"items_json": `["a","b","c"]`},
	}); err != nil {
		t.Fatal(err)
	}
	result := waitForDurableStatus(t, manager, "durable-map", RunStatusCompleted)
	if testResultValue(result.RunState, "mapper", "total_batches") != "2" {
		t.Fatalf("mapper.total_batches = %q, want 2", testResultValue(result.RunState, "mapper", "total_batches"))
	}
	if testResultValue(result.RunState, "mapper", "total_items") != "3" {
		t.Fatalf("mapper.total_items = %q, want 3", testResultValue(result.RunState, "mapper", "total_items"))
	}
	if got := countDurableNodeTraces(result.RunState, "mapper"); got != 1 {
		t.Fatalf("mapper trace count = %d, want 1", got)
	}
	if step.Count() != 2 {
		t.Fatalf("inline step count = %d, want 2", step.Count())
	}
}

// TestDurableManagerEmitsTraceSnapshotsOverLifecycle verifies that the durable
// manager pushes a trace envelope to the configured sink on every
// state-advancing persisted save. A suspend/resume/complete cycle touches
// enough distinct transitions that we can assert revisions stay monotonic,
// the final envelope matches the terminal state, and request metadata
// (AppID, BlueprintPath, Initiator) rides along.
func TestDurableManagerEmitsTraceSnapshotsOverLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	sink := newFakeTraceSink()
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(nil), DurableRunManagerConfig{
		MaxWorkers:   1,
		MaxQueueSize: 4,
		PollInterval: 10 * time.Millisecond,
		TraceSink:    sink,
	})
	defer manager.Close()

	const runID = "trace-lifecycle"
	const appID = 4001
	if _, err := manager.Submit(RunRequest{
		RunID:         runID,
		AppID:         appID,
		BlueprintPath: "main",
		Initiator:     "test:trace",
		BlueprintJSON: awaitSignalBlueprint(),
	}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, runID, RunStatusWaiting)

	if _, err := manager.Signal(signalRequest{
		IdempotencyKey: "trace-sig",
		AppID:          appID,
		RunID:          runID,
		SignalType:     "human_judgment.responded",
		CorrelationKey: fmt.Sprintf("%s:judge", runID),
		Payload:        map[string]string{"outcome": "1"},
	}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, runID, RunStatusCompleted)

	// waitForDurableStatus reads the on-disk record, which is written just
	// before the trace envelope is enqueued. Poll the sink until the
	// terminal-status envelope has landed so we don't race with the emit.
	var envelopes []TraceEnvelope
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		envelopes = sink.Snapshot()
		if len(envelopes) > 0 && envelopes[len(envelopes)-1].Run != nil && envelopes[len(envelopes)-1].Run.Status == "completed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(envelopes) < 4 {
		t.Fatalf("expected multiple trace snapshots across lifecycle, got %d: %+v", len(envelopes), envelopes)
	}

	var lastRevision int64
	sawRunning := false
	sawWaiting := false
	for i, env := range envelopes {
		if env.AppID != appID {
			t.Fatalf("envelope[%d].AppID = %d, want %d", i, env.AppID, appID)
		}
		if env.BlueprintPath != "main" {
			t.Fatalf("envelope[%d].BlueprintPath = %q, want %q", i, env.BlueprintPath, "main")
		}
		if env.Initiator != "test:trace" {
			t.Fatalf("envelope[%d].Initiator = %q, want test:trace", i, env.Initiator)
		}
		if env.Run == nil {
			t.Fatalf("envelope[%d].Run is nil", i)
		}
		if env.Run.ID != runID {
			t.Fatalf("envelope[%d].Run.ID = %q, want %q", i, env.Run.ID, runID)
		}
		if env.Revision <= lastRevision {
			t.Fatalf("envelope[%d] revision %d did not increase from %d", i, env.Revision, lastRevision)
		}
		lastRevision = env.Revision
		switch env.Run.Status {
		case string(RunStatusRunning):
			sawRunning = true
		case string(RunStatusWaiting):
			sawWaiting = true
		}
	}
	if !sawRunning {
		t.Fatal("expected at least one snapshot with status=running")
	}
	if !sawWaiting {
		t.Fatal("expected at least one snapshot with status=waiting")
	}
	final := envelopes[len(envelopes)-1]
	if final.Run.Status != "completed" {
		t.Fatalf("final envelope status = %q, want completed", final.Run.Status)
	}
	if got := testResultValue(final.Run, "judge", "outcome"); got != "1" {
		t.Fatalf("final envelope judge.outcome = %q, want 1", got)
	}
}

// TestDurableManagerEmitsDefensiveSnapshots verifies the envelope's RunState
// is a defensive copy — a consumer mutating the captured Run must not
// influence a subsequent snapshot of the same run.
func TestDurableManagerEmitsDefensiveSnapshots(t *testing.T) {
	tmpDir := t.TempDir()
	sink := newFakeTraceSink()
	outcome := &durableStaticExecutor{outputs: map[string]string{"status": "success", "outcome": "1"}}
	manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"outcome": outcome,
	}), DurableRunManagerConfig{
		MaxWorkers:   1,
		MaxQueueSize: 4,
		PollInterval: 10 * time.Millisecond,
		TraceSink:    sink,
	})
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{
		RunID:         "trace-defensive",
		AppID:         4002,
		BlueprintPath: "main",
		Initiator:     "test:defensive",
		BlueprintJSON: simpleSubmitBlueprint("outcome"),
	}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "trace-defensive", RunStatusCompleted)

	envelopes := sink.Snapshot()
	if len(envelopes) < 2 {
		t.Fatalf("expected multiple envelopes, got %d", len(envelopes))
	}
	// Mutate the earliest snapshot's Results; the final snapshot must be
	// unaffected.
	first := envelopes[0]
	if first.Run == nil || first.Run.Results == nil {
		t.Fatal("first envelope has no run/results")
	}
	first.Run.Results.SetField("__spurious__", "field", "mutation")
	first.Run.Status = "mutated"

	final := envelopes[len(envelopes)-1]
	if _, ok := final.Run.Results.Get("__spurious__", "field"); ok {
		t.Fatal("mutation to earlier envelope leaked into final envelope")
	}
	if final.Run.Status == "mutated" {
		t.Fatal("mutation to earlier envelope leaked into final envelope status")
	}
}

// TestDurablePreMatchedSignalCompletesNodeWithoutSuspending covers the
// executeNode branch where a matching signal is already in record.Signals
// by the time the suspending node runs: the node must complete immediately
// and never land in record.Checkpoint.Waiting.
func TestDurablePreMatchedSignalCompletesNodeWithoutSuspending(t *testing.T) {
	tmpDir := t.TempDir()
	gate := newDurableReleaseExecutor(map[string]string{"status": "success"})
	engine := durableTestEngine(map[string]dag.Executor{"gate": gate})
	manager := newDurableTestManager(t, tmpDir, engine, 1)
	defer manager.Close()

	runID := "pre-match-run"
	appID := 9100
	bp := mustMarshalBlueprint(dag.Blueprint{
		ID:      "pre-match",
		Version: 1,
		Nodes: []dag.NodeDef{
			{ID: "gate", Type: "gate", Config: map[string]interface{}{}},
			{ID: "judge", Type: "await_signal", Config: map[string]interface{}{
				"signal_type":      "human_judgment.responded",
				"required_payload": []string{"outcome"},
				"default_outputs":  map[string]string{"status": "responded"},
				"timeout_seconds":  60,
			}},
			returnSuccessNode("submit", "results.judge.outcome"),
		},
		Edges: []dag.EdgeDef{
			{From: "gate", To: "judge"},
			{From: "judge", To: "submit"},
		},
	})
	if _, err := manager.Submit(RunRequest{RunID: runID, AppID: appID, BlueprintJSON: bp}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-gate.started:
	case <-time.After(2 * time.Second):
		t.Fatal("gate executor never started")
	}

	// Deliver the judge's correlation-matched signal before judge has run.
	// AwaitSignalExecutor auto-derives "<app_id>:<run_id>:<node_id>".
	correlationKey := fmt.Sprintf("%s:judge", runID)
	if _, err := manager.Signal(signalRequest{
		IdempotencyKey: "pre-match-signal",
		RunID:          runID,
		SignalType:     "human_judgment.responded",
		CorrelationKey: correlationKey,
		Payload:        map[string]string{"outcome": "1"},
	}); err != nil {
		t.Fatal(err)
	}

	// Sanity: signal is persisted and no waiting entry exists yet.
	record, err := manager.store.loadRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := record.Signals["pre-match-signal"]; !ok {
		t.Fatalf("signal not stored before judge ran: %+v", record.Signals)
	}
	if len(record.Checkpoint.Waiting) != 0 {
		t.Fatalf("unexpected waiting entries before judge ran: %+v", record.Checkpoint.Waiting)
	}

	close(gate.release)

	result := waitForDurableStatus(t, manager, runID, RunStatusCompleted)
	if returnStringField(t, result.Return, "outcome") != "1" {
		t.Fatalf("outcome = %q, want 1", returnStringField(t, result.Return, "outcome"))
	}
	final, err := manager.store.loadRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if status := final.Checkpoint.Run.NodeStates["judge"].Status; status != "completed" {
		t.Fatalf("judge status = %q, want completed", status)
	}
	if consumedAt := final.Signals["pre-match-signal"].ConsumedAt; consumedAt == "" {
		t.Fatalf("pre-match signal was not marked consumed: %+v", final.Signals["pre-match-signal"])
	}

	events, err := manager.store.loadEvents(runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Type != "NodeSuspended" {
			continue
		}
		payload, ok := event.Payload.(map[string]interface{})
		if !ok {
			continue
		}
		if payload["node_id"] == "judge" {
			t.Fatalf("judge was suspended despite pre-delivered signal: %+v", event)
		}
	}
}

func TestDurableLoopedAwaitSignalRequiresFreshSignalPerIteration(t *testing.T) {
	tmpDir := t.TempDir()
	decide := &durableSequenceExecutor{
		outputs: []map[string]string{
			{"next": "loop"},
			{"next": "done"},
		},
	}
	manager := newDurableTestManager(t, tmpDir, durableTestEngine(map[string]dag.Executor{
		"decide": decide,
	}), 1)
	defer manager.Close()

	runID := "looped-await-signal"
	appID := 9101
	bp := mustMarshalBlueprint(dag.Blueprint{
		ID:      "looped-await-signal",
		Version: 1,
		Nodes: []dag.NodeDef{
			{ID: "judge", Type: "await_signal", Config: map[string]interface{}{
				"signal_type":      "human_judgment.responded",
				"required_payload": []string{"outcome"},
				"default_outputs":  map[string]string{"status": "responded"},
				"timeout_seconds":  60,
			}},
			{ID: "decide", Type: "decide", Config: map[string]interface{}{}},
			returnSuccessNode("submit", "results.judge.outcome"),
		},
		Edges: []dag.EdgeDef{
			{From: "judge", To: "decide"},
			{From: "decide", To: "judge", Condition: "results.decide.next == 'loop'", MaxTraversals: 1},
			{From: "decide", To: "submit", Condition: "results.decide.next == 'done'"},
		},
	})
	if _, err := manager.Submit(RunRequest{RunID: runID, AppID: appID, BlueprintJSON: bp}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, runID, RunStatusWaiting)

	correlationKey := fmt.Sprintf("%s:judge", runID)
	if _, err := manager.Signal(signalRequest{
		IdempotencyKey: "loop-signal-1",
		RunID:          runID,
		SignalType:     "human_judgment.responded",
		CorrelationKey: correlationKey,
		Payload:        map[string]string{"outcome": "1"},
	}); err != nil {
		t.Fatal(err)
	}

	secondWait := waitForDurableStatus(t, manager, runID, RunStatusWaiting)
	if got := secondWait.RunState.NodeStates["judge"].Status; got != "waiting" {
		t.Fatalf("judge status after first signal = %q, want waiting", got)
	}
	if got := decide.Count(); got != 1 {
		t.Fatalf("decide executor count after first signal = %d, want 1", got)
	}

	record, err := manager.store.loadRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if consumedAt := record.Signals["loop-signal-1"].ConsumedAt; consumedAt == "" {
		t.Fatalf("first signal was not marked consumed: %+v", record.Signals["loop-signal-1"])
	}

	if _, err := manager.Signal(signalRequest{
		IdempotencyKey: "loop-signal-2",
		RunID:          runID,
		SignalType:     "human_judgment.responded",
		CorrelationKey: correlationKey,
		Payload:        map[string]string{"outcome": "0"},
	}); err != nil {
		t.Fatal(err)
	}

	result := waitForDurableStatus(t, manager, runID, RunStatusCompleted)
	if returnStringField(t, result.Return, "outcome") != "0" {
		t.Fatalf("outcome = %q, want 0", returnStringField(t, result.Return, "outcome"))
	}
	if got := decide.Count(); got != 2 {
		t.Fatalf("decide executor count at completion = %d, want 2", got)
	}

	final, err := manager.store.loadRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if consumedAt := final.Signals["loop-signal-2"].ConsumedAt; consumedAt == "" {
		t.Fatalf("second signal was not marked consumed: %+v", final.Signals["loop-signal-2"])
	}
}
