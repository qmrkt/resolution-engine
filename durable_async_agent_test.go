package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qmrkt/resolution-engine/dag"
	"github.com/qmrkt/resolution-engine/executors"
)

// asyncAgentServer returns a minimal OpenAI chat-completions server that
// replies once with a text-mode answer. gate, when non-nil, blocks until
// closed so tests can observe the suspension window.
func asyncAgentServer(t *testing.T, answer string, gate <-chan struct{}, calls *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Fatalf("expected /chat/completions, got %s", r.URL.Path)
		}
		if calls != nil {
			calls.Add(1)
		}
		if gate != nil {
			select {
			case <-gate:
			case <-r.Context().Done():
				return
			}
		}
		writeAsyncAgentJSON(t, w, map[string]any{
			"choices": []map[string]any{
				{
					"finish_reason": "stop",
					"message": map[string]any{
						"role":    "assistant",
						"content": answer,
					},
				},
			},
			"usage": map[string]int{"prompt_tokens": 1, "completion_tokens": 2},
		})
	}))
}

func writeAsyncAgentJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("write response: %v", err)
	}
}

// newAsyncAgentTestManager builds a durable manager whose engine has a real
// agent_loop executor pointed at serverURL. The executor's signal sink is
// wired automatically inside NewDurableRunManager when runner.AgentExecutor()
// is non-nil.
func newAsyncAgentTestManager(t *testing.T, tmpDir, serverURL string, cfg DurableRunManagerConfig) *DurableRunManager {
	t.Helper()
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 10 * time.Millisecond
	}
	if cfg.MaxWorkers == 0 {
		cfg.MaxWorkers = 1
	}

	engine := dag.NewEngine(nil)
	agentExec := executors.NewAgentLoopExecutorWithConfig(executors.LLMCallExecutorConfig{
		OpenAIAPIKey:  "test-openai-key",
		OpenAIBaseURL: serverURL + "/chat/completions",
	}, engine)
	engine.RegisterExecutor("agent_loop", agentExec)
	engine.RegisterExecutor("return", executors.NewReturnExecutor())
	engine.RegisterExecutor("wait", executors.NewWaitExecutor())
	engine.RegisterExecutor("await_signal", executors.NewAwaitSignalExecutor())

	runner := &Runner{
		engine:    engine,
		dataDir:   filepath.Join(tmpDir, "evidence"),
		agentExec: agentExec,
	}

	manager, err := NewDurableRunManager(runner, filepath.Join(tmpDir, "store"), nil, "", cfg)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func asyncAgentBlueprint(t *testing.T) []byte {
	t.Helper()
	bp := dag.Blueprint{
		ID:      "async-agent",
		Version: 1,
		Nodes: []dag.NodeDef{
			{
				ID:   "agent",
				Type: "agent_loop",
				Config: map[string]any{
					"provider":        executors.LLMProviderOpenAI,
					"model":           "o4-mini",
					"prompt":          "Summarize in one word.",
					"output_mode":     executors.AgentOutputModeText,
					"async":           true,
					"timeout_seconds": 30,
				},
			},
		},
	}
	raw, err := json.Marshal(bp)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestDurableAsyncAgentCompletesViaSignalDelivery(t *testing.T) {
	tmpDir := t.TempDir()
	server := asyncAgentServer(t, "done", nil, nil)
	defer server.Close()
	manager := newAsyncAgentTestManager(t, tmpDir, server.URL, DurableRunManagerConfig{})
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{
		RunID:         "async-agent-ok",
		AppID:         7001,
		BlueprintJSON: asyncAgentBlueprint(t),
	}); err != nil {
		t.Fatal(err)
	}

	result := waitForDurableStatus(t, manager, "async-agent-ok", RunStatusCompleted)
	if result.RunState == nil {
		t.Fatal("missing run state on completion")
	}
	if got := result.RunState.NodeStates["agent"].Status; got != "completed" {
		t.Fatalf("agent node status = %q, want completed", got)
	}
	if got := testResultValue(result.RunState, "agent", "status"); got != "success" {
		t.Fatalf("agent.status = %q, want success", got)
	}
	if got := testResultValue(result.RunState, "agent", "text"); got != "done" {
		t.Fatalf("agent.text = %q, want done", got)
	}
	if got := result.RunState.NodeStates["agent"].Usage; got.InputTokens != 1 || got.OutputTokens != 2 {
		t.Fatalf("agent usage = %+v, want {InputTokens:1 OutputTokens:2}", got)
	}
	if got := result.RunState.Usage; got.InputTokens != 1 || got.OutputTokens != 2 {
		t.Fatalf("run usage = %+v, want {InputTokens:1 OutputTokens:2}", got)
	}
	if _, ok := result.RunState.Results.Get("agent", "_async_input_tokens"); ok {
		t.Fatalf("agent._async_input_tokens leaked into durable results: %+v", result.RunState.Results)
	}
	if _, ok := result.RunState.Results.Get("agent", "_async_output_tokens"); ok {
		t.Fatalf("agent._async_output_tokens leaked into durable results: %+v", result.RunState.Results)
	}
}

func TestDurableAsyncAgentSuspendsImmediatelyAndReleasesWorker(t *testing.T) {
	// Keep the async goroutine in flight and verify it does not pin the worker.
	tmpDir := t.TempDir()
	gate := make(chan struct{})
	gateClosed := false
	defer func() {
		if !gateClosed {
			close(gate)
		}
	}()
	var calls atomic.Int32
	server := asyncAgentServer(t, "done", gate, &calls)
	defer server.Close()
	manager := newAsyncAgentTestManager(t, tmpDir, server.URL, DurableRunManagerConfig{MaxWorkers: 1})
	defer manager.Close()

	// Register `outcome` so the simple test blueprint validates.
	manager.runner.engine.RegisterExecutor("outcome", &mockExecutor{
		outputs: map[string]string{"status": "success", "outcome": "1"},
	})

	if _, err := manager.Submit(RunRequest{
		RunID:         "async-agent-blocked",
		AppID:         7002,
		BlueprintJSON: asyncAgentBlueprint(t),
	}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "async-agent-blocked", RunStatusWaiting)

	// The run is already waiting, so the single worker can pick up another run.
	if _, err := manager.Submit(RunRequest{
		RunID:         "simple-while-agent-waiting",
		AppID:         7003,
		BlueprintJSON: simpleSubmitBlueprint("outcome"),
	}); err != nil {
		t.Fatal(err)
	}
	quick := waitForDurableStatus(t, manager, "simple-while-agent-waiting", RunStatusCompleted)
	if returnStringField(t, quick.Return, "outcome") != "1" {
		t.Fatalf("quick run outcome = %q, want 1", returnStringField(t, quick.Return, "outcome"))
	}

	// Let the async goroutine finish and resume the run.
	close(gate)
	gateClosed = true
	result := waitForDurableStatus(t, manager, "async-agent-blocked", RunStatusCompleted)
	if testResultValue(result.RunState, "agent", "text") != "done" {
		t.Fatalf("agent.text = %q, want done", testResultValue(result.RunState, "agent", "text"))
	}
	if calls.Load() != 1 {
		t.Fatalf("LLM calls = %d, want 1", calls.Load())
	}
}

func TestDurableAsyncAgentTimeoutResolvesThroughTimeoutOutputs(t *testing.T) {
	// The gate never opens, so the timeout path must resolve the node.
	tmpDir := t.TempDir()
	gate := make(chan struct{})
	// close(gate) must run before server.Close.
	server := asyncAgentServer(t, "irrelevant", gate, nil)
	defer server.Close()
	defer close(gate)
	manager := newAsyncAgentTestManager(t, tmpDir, server.URL, DurableRunManagerConfig{})
	defer manager.Close()

	bp := dag.Blueprint{
		ID:      "async-agent-timeout",
		Version: 1,
		Nodes: []dag.NodeDef{
			{
				ID:   "agent",
				Type: "agent_loop",
				Config: map[string]any{
					"provider":        executors.LLMProviderOpenAI,
					"model":           "o4-mini",
					"prompt":          "Never replies in time.",
					"output_mode":     executors.AgentOutputModeText,
					"async":           true,
					"timeout_seconds": 1,
				},
			},
			{ID: "cancel", Type: "return", Config: map[string]any{
				"value": map[string]any{
					"status": "cancelled",
					"reason": "agent timeout",
				},
			}},
		},
		Edges: []dag.EdgeDef{{From: "agent", To: "cancel", Condition: "results.agent.status == 'failed'"}},
	}
	bpJSON, err := json.Marshal(bp)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Submit(RunRequest{
		RunID:         "async-agent-timeout",
		AppID:         7004,
		BlueprintJSON: bpJSON,
	}); err != nil {
		t.Fatal(err)
	}

	result := waitForDurableStatus(t, manager, "async-agent-timeout", RunStatusCompleted)
	if testResultValue(result.RunState, "agent", "status") != "failed" {
		t.Fatalf("agent.status = %q, want failed", testResultValue(result.RunState, "agent", "status"))
	}
	if result.RunState.NodeStates["cancel"].Status != "completed" {
		t.Fatalf("cancel node status = %q, want completed", result.RunState.NodeStates["cancel"].Status)
	}
}

func TestDurableAsyncAgentConcurrentSignalBeatsSuspensionPersist(t *testing.T) {
	// A matching signal that wins the save race should complete the node.
	tmpDir := t.TempDir()
	gate := make(chan struct{})
	server := blockingAgentServer(gate)
	defer server.Close()
	defer close(gate)
	manager := newAsyncAgentTestManager(t, tmpDir, server.URL, DurableRunManagerConfig{})
	defer manager.Close()

	const runID = "async-agent-racy-signal"
	var injectOnce sync.Once
	setDurableBeforeSaveRun(t, manager, func(record *durableRunRecord) error {
		if record.Request.RunID != runID || len(record.Checkpoint.Waiting) == 0 {
			return nil
		}
		var hookErr error
		injectOnce.Do(func() {
			waiting := record.Checkpoint.Waiting["agent"]
			fresh, err := manager.store.loadRunLocked(runID)
			if err != nil {
				hookErr = err
				return
			}
			ensureDurableRecordMaps(fresh)
			fresh.Signals[waiting.CorrelationKey] = signalRequest{
				IdempotencyKey: waiting.CorrelationKey,
				RunID:          runID,
				SignalType:     executors.AgentDoneSignalType,
				CorrelationKey: waiting.CorrelationKey,
				Payload: map[string]string{
					"status": "success",
					"text":   "race-winner",
				},
				Usage: dag.TokenUsage{
					InputTokens:  7,
					OutputTokens: 11,
				},
			}
			fresh.Revision++
			fresh.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
			if err := writeJSONAtomic(manager.store.runPath(runID), fresh); err != nil {
				hookErr = err
				return
			}
			hookErr = fmt.Errorf("%w: injected concurrent signal", errDurableStaleRecord)
		})
		return hookErr
	})

	if _, err := manager.Submit(RunRequest{
		RunID:         runID,
		AppID:         7006,
		BlueprintJSON: asyncAgentBlueprint(t),
	}); err != nil {
		t.Fatal(err)
	}

	result := waitForDurableStatus(t, manager, runID, RunStatusCompleted)
	if got := testResultValue(result.RunState, "agent", "text"); got != "race-winner" {
		t.Fatalf("agent.text = %q, want race-winner", got)
	}
	if state := result.RunState.NodeStates["agent"].Status; state != "completed" {
		t.Fatalf("agent node status = %q, want completed", state)
	}
	if usage := result.RunState.NodeStates["agent"].Usage; usage.InputTokens != 7 || usage.OutputTokens != 11 {
		t.Fatalf("agent usage = %+v, want {InputTokens:7 OutputTokens:11}", usage)
	}
}

func TestDurableAsyncAgentStaleSignalIsDropped(t *testing.T) {
	// A bogus correlation key should not resolve the waiting agent.
	tmpDir := t.TempDir()
	gate := make(chan struct{})
	gateClosed := false
	server := asyncAgentServer(t, "done", gate, nil)
	defer server.Close()
	defer func() {
		if !gateClosed {
			close(gate)
		}
	}()
	manager := newAsyncAgentTestManager(t, tmpDir, server.URL, DurableRunManagerConfig{})
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{
		RunID:         "async-agent-stale",
		AppID:         7005,
		BlueprintJSON: asyncAgentBlueprint(t),
	}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "async-agent-stale", RunStatusWaiting)

	// Wrong correlation: stored, but should not advance the run.
	if _, err := manager.Signal(signalRequest{
		IdempotencyKey: "stale-1",
		AppID:          7005,
		RunID:          "async-agent-stale",
		SignalType:     executors.AgentDoneSignalType,
		CorrelationKey: "bogus:correlation:key",
		Payload:        map[string]string{"status": "success", "text": "hijacked"},
	}); err != nil {
		t.Fatal(err)
	}

	// The run should still be waiting.
	time.Sleep(50 * time.Millisecond)
	snapshot, ok := manager.Get("async-agent-stale")
	if !ok {
		t.Fatal("run disappeared")
	}
	if snapshot.Status != RunStatusWaiting {
		t.Fatalf("status after stale signal = %q, want waiting", snapshot.Status)
	}

	// The real signal should still win.
	close(gate)
	gateClosed = true
	result := waitForDurableStatus(t, manager, "async-agent-stale", RunStatusCompleted)
	if got := testResultValue(result.RunState, "agent", "text"); got != "done" {
		t.Fatalf("agent.text = %q, want 'done' — stale signal may have resolved the node", got)
	}
}

// blockingAgentServer holds requests until gate is closed.
func blockingAgentServer(gate <-chan struct{}) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-gate:
		case <-r.Context().Done():
		}
	}))
}

func TestDurableAsyncAgentCancelClearsInFlightRegistry(t *testing.T) {
	// Cancel should clear the in-flight registry entry for the waiting run.
	tmpDir := t.TempDir()
	gate := make(chan struct{})
	server := blockingAgentServer(gate)
	// close(gate) must run before server.Close.
	defer server.Close()
	defer close(gate)
	manager := newAsyncAgentTestManager(t, tmpDir, server.URL, DurableRunManagerConfig{})
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{
		RunID:         "async-agent-cancel",
		AppID:         7010,
		BlueprintJSON: asyncAgentBlueprint(t),
	}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "async-agent-cancel", RunStatusWaiting)

	if got := manager.runner.AgentExecutor().InFlightCount(); got != 1 {
		t.Fatalf("InFlightCount before cancel = %d, want 1", got)
	}

	if _, ok := manager.Cancel("async-agent-cancel"); !ok {
		t.Fatal("cancel did not find run")
	}
	waitForDurableStatus(t, manager, "async-agent-cancel", RunStatusCancelled)

	if got := manager.runner.AgentExecutor().InFlightCount(); got != 0 {
		t.Fatalf("InFlightCount after cancel = %d, want 0", got)
	}
}

func TestDurableAsyncAgentShutdownClearsInFlightRegistry(t *testing.T) {
	tmpDir := t.TempDir()
	gate := make(chan struct{})
	server := blockingAgentServer(gate)
	defer server.Close()
	defer close(gate)
	manager := newAsyncAgentTestManager(t, tmpDir, server.URL, DurableRunManagerConfig{})
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{
		RunID:         "async-agent-shutdown",
		AppID:         7013,
		BlueprintJSON: asyncAgentBlueprint(t),
	}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "async-agent-shutdown", RunStatusWaiting)

	if got := manager.runner.AgentExecutor().InFlightCount(); got != 1 {
		t.Fatalf("InFlightCount before shutdown = %d, want 1", got)
	}
	manager.BeginShutdown()
	waitForAsyncAgentRegistry(t, manager.runner.AgentExecutor(), 0)
}

func TestDurableAsyncAgentCancelPrePersistClearsInFlightRegistry(t *testing.T) {
	// Cancel must also clear dispatched work before the waiting save lands.
	tmpDir := t.TempDir()
	gate := make(chan struct{})
	server := blockingAgentServer(gate)
	defer server.Close()
	defer close(gate)
	manager := newAsyncAgentTestManager(t, tmpDir, server.URL, DurableRunManagerConfig{})
	defer manager.Close()

	const runID = "async-agent-cancel-prepersist"
	waitingPersistStarted := make(chan struct{}, 1)
	releaseWaitingPersist := make(chan struct{})
	var blockOnce sync.Once
	setDurableBeforeSaveRun(t, manager, func(record *durableRunRecord) error {
		if record.Request.RunID != runID || len(record.Checkpoint.Waiting) == 0 {
			return nil
		}
		blockOnce.Do(func() {
			waitingPersistStarted <- struct{}{}
		})
		<-releaseWaitingPersist
		return nil
	})

	if _, err := manager.Submit(RunRequest{
		RunID:         runID,
		AppID:         7014,
		BlueprintJSON: asyncAgentBlueprint(t),
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-waitingPersistStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("waiting persist did not start")
	}
	if got := manager.runner.AgentExecutor().InFlightCount(); got != 1 {
		t.Fatalf("InFlightCount before cancel = %d, want 1", got)
	}

	cancelDone := make(chan struct{})
	go func() {
		defer close(cancelDone)
		if _, ok := manager.Cancel(runID); !ok {
			t.Errorf("cancel did not find run")
		}
	}()

	waitForAsyncAgentRegistry(t, manager.runner.AgentExecutor(), 0)
	close(releaseWaitingPersist)

	select {
	case <-cancelDone:
	case <-time.After(2 * time.Second):
		t.Fatal("cancel did not finish")
	}

	result := waitForDurableStatus(t, manager, runID, RunStatusCancelled)
	if result.Status != RunStatusCancelled {
		t.Fatalf("final status = %q, want cancelled", result.Status)
	}
}

func backEdgeAsyncAgentBlueprint(t *testing.T) []byte {
	t.Helper()
	// Drives a back-edge onto a waiting agent so reset/cancel behavior is exercised.
	bp := dag.Blueprint{
		ID:      "async-agent-backedge",
		Version: 1,
		Nodes: []dag.NodeDef{
			{ID: "counter", Type: "bump_counter", Config: map[string]any{}},
			{
				ID:   "agent",
				Type: "agent_loop",
				Config: map[string]any{
					"provider":        executors.LLMProviderOpenAI,
					"model":           "o4-mini",
					"prompt":          "say done",
					"output_mode":     executors.AgentOutputModeText,
					"async":           true,
					"timeout_seconds": 30,
				},
			},
		},
		Edges: []dag.EdgeDef{
			{From: "counter", To: "agent"},
			{From: "agent", To: "counter", MaxTraversals: 1},
		},
	}
	raw, err := json.Marshal(bp)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type bumpCounterExecutor struct {
	calls atomic.Int32
}

func (e *bumpCounterExecutor) Execute(_ context.Context, _ dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	n := strconv.Itoa(int(e.calls.Add(1)))
	inv.Results.SetField("counter", "count", n)
	return dag.ExecutorResult{Outputs: map[string]string{"count": n}}, nil
}

func TestDurableAsyncAgentCancelCorrelationPrimitive(t *testing.T) {
	// CancelCorrelation should remove the waiting agent's canceller exactly once.
	tmpDir := t.TempDir()
	gate := make(chan struct{})
	server := blockingAgentServer(gate)
	defer server.Close()
	defer close(gate)
	manager := newAsyncAgentTestManager(t, tmpDir, server.URL, DurableRunManagerConfig{})
	defer manager.Close()
	manager.runner.engine.RegisterExecutor("bump_counter", &bumpCounterExecutor{})

	if _, err := manager.Submit(RunRequest{
		RunID:         "async-agent-cancel-primitive",
		AppID:         7011,
		BlueprintJSON: backEdgeAsyncAgentBlueprint(t),
	}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "async-agent-cancel-primitive", RunStatusWaiting)

	record, err := manager.store.loadRun("async-agent-cancel-primitive")
	if err != nil {
		t.Fatal(err)
	}
	waiting, ok := record.Checkpoint.Waiting["agent"]
	if !ok {
		t.Fatalf("expected agent waiting entry, got %+v", record.Checkpoint.Waiting)
	}
	agent := manager.runner.AgentExecutor()
	if got := agent.InFlightCount(); got != 1 {
		t.Fatalf("InFlightCount before cancel = %d, want 1", got)
	}
	if !agent.CancelCorrelation(waiting.CorrelationKey) {
		t.Fatal("CancelCorrelation returned false; canceller was not registered")
	}
	if agent.CancelCorrelation(waiting.CorrelationKey) {
		t.Fatal("CancelCorrelation returned true on second call; entry should be gone")
	}
	if got := agent.InFlightCount(); got != 0 {
		t.Fatalf("InFlightCount after cancel = %d, want 0", got)
	}
}

func TestDurableEvaluateEdgesResetsWaitingNodeAndCancelsGoroutine(t *testing.T) {
	// Re-entering a waiting node through a back-edge should reset and cancel it.
	tmpDir := t.TempDir()
	gate := make(chan struct{})
	server := blockingAgentServer(gate)
	defer server.Close()
	defer close(gate)
	manager := newAsyncAgentTestManager(t, tmpDir, server.URL, DurableRunManagerConfig{})
	defer manager.Close()
	manager.runner.engine.RegisterExecutor("bump_counter", &bumpCounterExecutor{})

	if _, err := manager.Submit(RunRequest{
		RunID:         "async-agent-backedge",
		AppID:         7012,
		BlueprintJSON: backEdgeAsyncAgentBlueprint(t),
	}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, manager, "async-agent-backedge", RunStatusWaiting)

	record, err := manager.store.loadRun("async-agent-backedge")
	if err != nil {
		t.Fatal(err)
	}
	waiting, ok := record.Checkpoint.Waiting["agent"]
	if !ok {
		t.Fatalf("expected agent waiting entry, got %+v", record.Checkpoint.Waiting)
	}
	if manager.runner.AgentExecutor().InFlightCount() != 1 {
		t.Fatal("expected one in-flight agent goroutine")
	}

	// Drive the back-edge manually to hit the reset path.
	record.Checkpoint.Completed["agent"] = true
	if err := manager.evaluateOutgoingEdges(record, "agent"); err != nil {
		t.Fatalf("evaluateOutgoingEdges: %v", err)
	}

	// Then simulate counter completing into the waiting agent.
	if err := manager.evaluateOutgoingEdges(record, "counter"); err != nil {
		t.Fatalf("evaluateOutgoingEdges(counter): %v", err)
	}

	if _, stillWaiting := record.Checkpoint.Waiting["agent"]; stillWaiting {
		t.Fatalf("agent should have been removed from Waiting: %+v", record.Checkpoint.Waiting)
	}
	if state := record.Checkpoint.Run.NodeStates["agent"].Status; state != "pending" {
		t.Fatalf("agent node status after reset = %q, want pending", state)
	}
	if got := manager.runner.AgentExecutor().InFlightCount(); got != 0 {
		t.Fatalf("InFlightCount after back-edge reset = %d, want 0", got)
	}
	if agent := manager.runner.AgentExecutor(); agent.CancelCorrelation(waiting.CorrelationKey) {
		t.Fatal("canceller should already be gone; back-edge reset should have invoked it")
	}
}

func TestDurableAsyncAgentRestartFailsInterruptedWait(t *testing.T) {
	// Restart should fail an orphaned async-agent wait immediately.
	tmpDir := t.TempDir()
	gate := make(chan struct{})
	server := blockingAgentServer(gate)
	// close(gate) must run before server.Close.
	defer server.Close()
	defer close(gate)

	first := newAsyncAgentTestManager(t, tmpDir, server.URL, DurableRunManagerConfig{})
	const runID = "async-agent-restart-fail"
	if _, err := first.Submit(RunRequest{
		RunID:         runID,
		AppID:         7020,
		BlueprintJSON: asyncAgentBlueprint(t),
	}); err != nil {
		t.Fatal(err)
	}
	waitForDurableStatus(t, first, runID, RunStatusWaiting)
	first.Close()

	second := newAsyncAgentTestManager(t, tmpDir, server.URL, DurableRunManagerConfig{})
	defer second.Close()

	result := waitForDurableStatus(t, second, runID, RunStatusCompleted)
	if got := testResultValue(result.RunState, "agent", "status"); got != "failed" {
		t.Fatalf("agent.status = %q, want failed", got)
	}
	if got := testResultValue(result.RunState, "agent", "error"); !strings.Contains(got, "interrupted by manager restart") {
		t.Fatalf("agent.error = %q, want mention of restart interruption", got)
	}
	if got := testResultValue(result.RunState, "agent", "outcome"); got != "inconclusive" {
		t.Fatalf("agent.outcome = %q, want inconclusive", got)
	}
	if got := result.RunState.NodeStates["agent"].Status; got != "completed" {
		t.Fatalf("agent node status = %q, want completed", got)
	}
}

func TestDurableRestartInterruptedWaitCoexistsWithParkedTimer(t *testing.T) {
	// On restart, agent waits should fail while pure timer waits survive.
	tmpDir := t.TempDir()
	gate := make(chan struct{})
	server := blockingAgentServer(gate)
	defer server.Close()
	defer close(gate)

	bp := dag.Blueprint{
		ID:      "async-agent-restart-mixed",
		Version: 1,
		Nodes: []dag.NodeDef{
			{
				ID:   "agent",
				Type: "agent_loop",
				Config: map[string]any{
					"provider":        executors.LLMProviderOpenAI,
					"model":           "o4-mini",
					"prompt":          "never answers",
					"output_mode":     executors.AgentOutputModeText,
					"async":           true,
					"timeout_seconds": 300,
				},
			},
			{
				ID:   "parked",
				Type: "wait",
				Config: map[string]any{
					"duration_seconds":   300,
					"max_inline_seconds": -1, // force durable suspension
				},
			},
		},
	}
	bpJSON, err := json.Marshal(bp)
	if err != nil {
		t.Fatal(err)
	}

	first := newAsyncAgentTestManager(t, tmpDir, server.URL, DurableRunManagerConfig{})
	const runID = "async-agent-restart-mixed"
	if _, err := first.Submit(RunRequest{
		RunID:         runID,
		AppID:         7021,
		BlueprintJSON: bpJSON,
	}); err != nil {
		t.Fatal(err)
	}
	// Both roots may suspend before shutdown; the mixed recovery state matters.
	waitForDurableStatus(t, first, runID, RunStatusWaiting)
	rec, err := first.store.loadRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	agentWait, ok := rec.Checkpoint.Waiting["agent"]
	if !ok {
		t.Fatalf("want agent in Waiting pre-restart, got %+v", rec.Checkpoint.Waiting)
	}
	if agentWait.RecoveryOwner != executors.AgentLoopRecoveryOwner {
		t.Fatalf("agent RecoveryOwner = %q, want %q",
			agentWait.RecoveryOwner, executors.AgentLoopRecoveryOwner)
	}
	if parkedWait, ok := rec.Checkpoint.Waiting["parked"]; !ok {
		t.Fatalf("want parked timer in Waiting pre-restart, got %+v", rec.Checkpoint.Waiting)
	} else if parkedWait.RecoveryOwner != "" {
		t.Fatalf("parked RecoveryOwner = %q, want empty", parkedWait.RecoveryOwner)
	}
	first.Close()

	second := newAsyncAgentTestManager(t, tmpDir, server.URL, DurableRunManagerConfig{})
	defer second.Close()

	// After recovery, the run should settle back into Waiting on the timer.
	settled := waitForDurableStatus(t, second, runID, RunStatusWaiting)
	if got := testResultValue(settled.RunState, "agent", "status"); got != "failed" {
		t.Fatalf("agent.status = %q, want failed", got)
	}
	if got := testResultValue(settled.RunState, "agent", "error"); !strings.Contains(got, "interrupted by manager restart") {
		t.Fatalf("agent.error = %q, want mention of restart interruption", got)
	}
	if got := settled.RunState.NodeStates["agent"].Status; got != "completed" {
		t.Fatalf("agent node status = %q, want completed", got)
	}
	if got := settled.RunState.NodeStates["parked"].Status; got != "waiting" {
		t.Fatalf("parked node status = %q, want waiting", got)
	}
	rec2, err := second.store.loadRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if _, still := rec2.Checkpoint.Waiting["agent"]; still {
		t.Fatalf("agent should be removed from Waiting after restart fail: %+v", rec2.Checkpoint.Waiting)
	}
	if _, ok := rec2.Checkpoint.Waiting["parked"]; !ok {
		t.Fatalf("parked wait should survive restart: %+v", rec2.Checkpoint.Waiting)
	}
}

func waitForAsyncAgentRegistry(t *testing.T, exec *executors.AgentLoopExecutor, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := exec.InFlightCount(); got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("InFlightCount = %d, want %d", exec.InFlightCount(), want)
}
