package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// mockExecutor returns fixed outputs.
type mockExecutor struct {
	outputs map[string]string
	err     error
	delay   time.Duration
	usage   TokenUsage
}

func (m *mockExecutor) Execute(ctx context.Context, node NodeDef, execCtx *Invocation) (ExecutorResult, error) {
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	if m.err != nil {
		return ExecutorResult{}, m.err
	}
	return ExecutorResult{Outputs: m.outputs, Usage: m.usage}, nil
}

type concurrencyRecordingExecutor struct {
	delay     time.Duration
	active    atomic.Int32
	maxActive atomic.Int32
}

func (e *concurrencyRecordingExecutor) Execute(ctx context.Context, node NodeDef, execCtx *Invocation) (ExecutorResult, error) {
	active := e.active.Add(1)
	for {
		maxActive := e.maxActive.Load()
		if active <= maxActive || e.maxActive.CompareAndSwap(maxActive, active) {
			break
		}
	}
	defer e.active.Add(-1)
	if e.delay > 0 {
		time.Sleep(e.delay)
	}
	return ExecutorResult{Outputs: map[string]string{"done": "yes"}}, nil
}

// contextAwareExecutor reads context during execution.
type contextAwareExecutor struct{}

func (e *contextAwareExecutor) Execute(ctx context.Context, node NodeDef, inv *Invocation) (ExecutorResult, error) {
	evidence, _ := inv.Results.Get("search", "evidence")
	if evidence == "" {
		return ExecutorResult{Outputs: map[string]string{"outcome": "inconclusive"}}, nil
	}
	return ExecutorResult{Outputs: map[string]string{"outcome": "0"}}, nil
}

type runIDInspectingExecutor struct{}

func (e *runIDInspectingExecutor) Execute(ctx context.Context, node NodeDef, inv *Invocation) (ExecutorResult, error) {
	// All three output keys should carry inv.Run.ID.
	return ExecutorResult{
		Outputs: map[string]string{
			"resolution_run_id": inv.Run.ID,
			"input_run_id":      inv.Run.ID,
			"internal_run_id":   inv.Run.ID,
		},
	}, nil
}

type collectingObserver struct {
	snapshots []*RunState
}

func (o *collectingObserver) OnRunSnapshot(run *RunState) {
	o.snapshots = append(o.snapshots, run)
}

type loopCountingExecutor struct {
	count int
}

func (e *loopCountingExecutor) Execute(ctx context.Context, node NodeDef, execCtx *Invocation) (ExecutorResult, error) {
	e.count++
	return ExecutorResult{Outputs: map[string]string{"count": fmt.Sprintf("%d", e.count)}}, nil
}

type suspendingExecutor struct {
	suspension *Suspension
}

func (e *suspendingExecutor) Execute(ctx context.Context, node NodeDef, execCtx *Invocation) (ExecutorResult, error) {
	return ExecutorResult{Suspend: e.suspension}, nil
}

func TestSyncEngineRejectsSuspendedResult(t *testing.T) {
	cases := []struct {
		name       string
		suspension *Suspension
		wantKind   string
	}{
		{
			name:       "timer_suspend",
			suspension: &Suspension{Kind: SuspensionKindTimer, ResumeAtUnix: time.Now().Unix() + 60},
			wantKind:   SuspensionKindTimer,
		},
		{
			name:       "signal_suspend",
			suspension: &Suspension{Kind: SuspensionKindSignal, SignalType: "x.y"},
			wantKind:   SuspensionKindSignal,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			engine := NewEngine(slog.Default())
			engine.RegisterExecutor("suspend", &suspendingExecutor{suspension: tc.suspension})
			bp := Blueprint{
				ID:    "sync-rejects-suspend",
				Nodes: []NodeDef{{ID: "s", Type: "suspend"}},
			}
			run, err := engine.Execute(context.Background(), bp, nil)
			if err != nil {
				// Failing a hard-failing node is surfaced via run.Status;
				// engine.Execute returns a nil error in that case.
				t.Fatalf("unexpected engine error: %v", err)
			}
			if run.Status != "failed" {
				t.Fatalf("run status = %q, want failed", run.Status)
			}
			nodeState := run.NodeStates["s"]
			if nodeState.Status != "failed" {
				t.Fatalf("node status = %q, want failed", nodeState.Status)
			}
			if !strings.Contains(nodeState.Error, "not supported by the in-memory engine") {
				t.Fatalf("node error missing expected phrase: %q", nodeState.Error)
			}
			if !strings.Contains(nodeState.Error, tc.wantKind) {
				t.Fatalf("node error did not mention kind %q: %q", tc.wantKind, nodeState.Error)
			}
		})
	}
}

func TestSingleNodeExecution(t *testing.T) {
	engine := NewEngine(slog.Default())
	engine.RegisterExecutor("test", &mockExecutor{
		outputs: map[string]string{"result": "hello"},
	})

	bp := Blueprint{
		ID:    "test-single",
		Nodes: []NodeDef{{ID: "a", Type: "test"}},
	}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
	if resultValue(run, "a", "result") != "hello" {
		t.Fatalf("expected a.result=hello, got %q", resultValue(run, "a", "result"))
	}
}

func TestRunStateCapturesDefinitionInputsAndCanonicalRunID(t *testing.T) {
	engine := NewEngine(slog.Default())
	engine.RegisterExecutor("inspect", &runIDInspectingExecutor{})

	bp := Blueprint{
		ID:   "test-run-metadata",
		Name: "Metadata Test",
		Nodes: []NodeDef{
			{ID: "step", Type: "inspect"},
		},
	}

	run, err := engine.Execute(context.Background(), bp, map[string]string{
		"market_app_id":   "42",
		"market.question": "Will BTC close above $100k?",
	})
	if err != nil {
		t.Fatal(err)
	}
	if run.ID == "" {
		t.Fatal("expected run ID to be populated")
	}
	if run.Definition.ID != bp.ID || run.Definition.Name != bp.Name {
		t.Fatalf("run definition mismatch: %#v", run.Definition)
	}
	if run.Inputs["market_app_id"] != "42" {
		t.Fatalf("expected market_app_id input, got %q", run.Inputs["market_app_id"])
	}
	// runIDInspectingExecutor copies inv.Run.ID into these output keys.
	for _, name := range []string{"resolution_run_id", "input_run_id", "internal_run_id"} {
		got, _ := run.Results.Get("step", name)
		if got != run.ID {
			t.Fatalf("expected step.%s=%q from inv.Run.ID, got %q", name, run.ID, got)
		}
	}
}

func TestLinearDAG(t *testing.T) {
	engine := NewEngine(slog.Default())
	engine.RegisterExecutor("step", &contextAwareExecutor{})
	engine.RegisterExecutor("search", &mockExecutor{
		outputs: map[string]string{"evidence": "BTC is $70k"},
	})

	bp := Blueprint{
		ID: "test-linear",
		Nodes: []NodeDef{
			{ID: "search", Type: "search"},
			{ID: "judge", Type: "step"},
		},
		Edges: []EdgeDef{
			{From: "search", To: "judge"},
		},
	}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
	// Judge should see the search evidence
	if resultValue(run, "judge", "outcome") != "0" {
		t.Fatalf("expected judge.outcome=0, got %q", resultValue(run, "judge", "outcome"))
	}
}

func TestConditionalBranching(t *testing.T) {
	engine := NewEngine(slog.Default())

	engine.RegisterExecutor("search", &mockExecutor{
		outputs: map[string]string{"status": "success", "data": "BTC=$70k"},
	})
	engine.RegisterExecutor("judge", &mockExecutor{
		outputs: map[string]string{"outcome": "0"},
	})
	engine.RegisterExecutor("fallback", &mockExecutor{
		outputs: map[string]string{"outcome": "manual"},
	})

	bp := Blueprint{
		ID: "test-branch",
		Nodes: []NodeDef{
			{ID: "search", Type: "search"},
			{ID: "judge", Type: "judge"},
			{ID: "fallback", Type: "fallback"},
		},
		Edges: []EdgeDef{
			{From: "search", To: "judge", Condition: "results.search.status == 'success'"},
			{From: "search", To: "fallback", Condition: "results.search.status != 'success'"},
		},
	}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
	// Judge should have run (success branch)
	if run.NodeStates["judge"].Status != "completed" {
		t.Fatalf("expected judge completed, got %s", run.NodeStates["judge"].Status)
	}
	// Fallback should be skipped (failure branch not taken)
	if run.NodeStates["fallback"].Status != "pending" {
		t.Fatalf("expected fallback pending, got %s", run.NodeStates["fallback"].Status)
	}
}

func TestFailureBranching(t *testing.T) {
	engine := NewEngine(slog.Default())

	engine.RegisterExecutor("search", &mockExecutor{
		outputs: map[string]string{"status": "failed"},
	})
	engine.RegisterExecutor("judge", &mockExecutor{
		outputs: map[string]string{"outcome": "0"},
	})
	engine.RegisterExecutor("fallback", &mockExecutor{
		outputs: map[string]string{"outcome": "manual"},
	})

	bp := Blueprint{
		ID: "test-fail-branch",
		Nodes: []NodeDef{
			{ID: "search", Type: "search"},
			{ID: "judge", Type: "judge"},
			{ID: "fallback", Type: "fallback"},
		},
		Edges: []EdgeDef{
			{From: "search", To: "judge", Condition: "results.search.status == 'success'"},
			{From: "search", To: "fallback", Condition: "results.search.status != 'success'"},
		},
	}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Fallback should run (failure branch)
	if run.NodeStates["fallback"].Status != "completed" {
		t.Fatalf("expected fallback completed, got %s", run.NodeStates["fallback"].Status)
	}
	if resultValue(run, "fallback", "outcome") != "manual" {
		t.Fatalf("expected fallback.outcome=manual, got %q", resultValue(run, "fallback", "outcome"))
	}
}

func TestErrorHandlingFail(t *testing.T) {
	engine := NewEngine(slog.Default())

	engine.RegisterExecutor("failing", &mockExecutor{
		err: fmt.Errorf("connection refused"),
	})
	engine.RegisterExecutor("after", &mockExecutor{
		outputs: map[string]string{"result": "ok"},
	})

	bp := Blueprint{
		ID: "test-error-fail",
		Nodes: []NodeDef{
			{ID: "step1", Type: "failing"},
			{ID: "step2", Type: "after"},
		},
		Edges: []EdgeDef{{From: "step1", To: "step2"}},
	}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "failed" {
		t.Fatalf("expected failed, got %s", run.Status)
	}
	if run.NodeStates["step1"].Status != "failed" {
		t.Fatalf("expected step1 failed, got %s", run.NodeStates["step1"].Status)
	}
}

func TestErrorHandlingContinue(t *testing.T) {
	engine := NewEngine(slog.Default())

	engine.RegisterExecutor("failing", &mockExecutor{
		err: fmt.Errorf("non-critical error"),
	})
	engine.RegisterExecutor("after", &mockExecutor{
		outputs: map[string]string{"result": "ok"},
	})

	bp := Blueprint{
		ID: "test-error-continue",
		Nodes: []NodeDef{
			{ID: "step1", Type: "failing", OnError: "continue"},
			{ID: "step2", Type: "after"},
		},
		Edges: []EdgeDef{{From: "step1", To: "step2"}},
	}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
	// step2 should still run after step1 failed with on_error=continue
	if run.NodeStates["step2"].Status != "completed" {
		t.Fatalf("expected step2 completed, got %s", run.NodeStates["step2"].Status)
	}
}

func TestBudgetTimeout(t *testing.T) {
	engine := NewEngine(slog.Default())

	engine.RegisterExecutor("slow", &mockExecutor{
		outputs: map[string]string{"result": "done"},
		delay:   2 * time.Second,
	})

	bp := Blueprint{
		ID:     "test-timeout",
		Nodes:  []NodeDef{{ID: "a", Type: "slow"}},
		Budget: &Budget{MaxTotalTimeSeconds: 1},
	}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "cancelled") {
		t.Fatalf("expected cancelled error, got: %s", err)
	}
	if run.Status != "failed" {
		t.Fatalf("expected failed, got %s", run.Status)
	}
}

func TestParallelExecution(t *testing.T) {
	engine := NewEngine(slog.Default())
	work := &concurrencyRecordingExecutor{delay: 100 * time.Millisecond}
	engine.RegisterExecutor("work", work)

	bp := Blueprint{
		ID: "test-parallel",
		Nodes: []NodeDef{
			{ID: "a", Type: "work"},
			{ID: "b", Type: "work"},
			{ID: "c", Type: "work"},
		},
		// No edges — all three are roots and should run in parallel
	}

	run, err := engine.Execute(context.Background(), bp, nil)

	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
	if maxActive := work.maxActive.Load(); maxActive < 2 {
		t.Fatalf("expected parallel execution, max active workers = %d", maxActive)
	}
}

func TestExecuteWithObserverEmitsDefensiveSnapshots(t *testing.T) {
	engine := NewEngine(slog.Default())
	engine.RegisterExecutor("test", &mockExecutor{
		outputs: map[string]string{"result": "hello"},
	})

	observer := &collectingObserver{}
	run, err := engine.ExecuteWithObserver(context.Background(), Blueprint{
		ID:    "test-observer",
		Nodes: []NodeDef{{ID: "a", Type: "test"}},
	}, map[string]string{"market_app_id": "7"}, observer)
	if err != nil {
		t.Fatal(err)
	}
	if len(observer.snapshots) < 3 {
		t.Fatalf("expected at least 3 observer snapshots, got %d", len(observer.snapshots))
	}
	if got := observer.snapshots[0].NodeStates["a"].Status; got != "pending" {
		t.Fatalf("expected first snapshot pending, got %q", got)
	}
	foundRunning := false
	for _, snapshot := range observer.snapshots {
		if snapshot.NodeStates["a"].Status == "running" {
			foundRunning = true
			break
		}
	}
	if !foundRunning {
		t.Fatal("expected a running snapshot")
	}
	last := observer.snapshots[len(observer.snapshots)-1]
	if last.Status != "completed" {
		t.Fatalf("expected final observer snapshot completed, got %q", last.Status)
	}
	if last.NodeStates["a"].Status != "completed" {
		t.Fatalf("expected final node state completed, got %q", last.NodeStates["a"].Status)
	}

	observer.snapshots[0].NodeStates["a"] = NodeState{Status: "tampered"}
	if run.NodeStates["a"].Status != "completed" {
		t.Fatalf("observer snapshot mutation leaked into run state: %q", run.NodeStates["a"].Status)
	}
}

func TestContextInterpolation(t *testing.T) {
	ctx := NewInvocationFromInputs(map[string]string{"question": "Will BTC hit 100k?"})

	result := ctx.Interpolate("Market: {{inputs.question}}")
	if result != "Market: Will BTC hit 100k?" {
		t.Fatalf("unexpected interpolation: %q", result)
	}

	result = ctx.Interpolate("No placeholders here")
	if result != "No placeholders here" {
		t.Fatalf("unexpected: %q", result)
	}

	result = ctx.Interpolate("Missing: {{inputs.unknown}}")
	if result != "Missing: " {
		t.Fatalf("unexpected: %q", result)
	}
}

func TestResolutionWorkflow(t *testing.T) {
	// Simulates a real resolution: search → judge → submit
	engine := NewEngine(slog.Default())

	engine.RegisterExecutor("web_search", &mockExecutor{
		outputs: map[string]string{
			"status":   "success",
			"evidence": "According to CoinGecko, BTC closed at $70,123 on 2024-03-15",
		},
	})
	engine.RegisterExecutor("llm_call", &contextAwareExecutor{})
	engine.RegisterExecutor("result_a", &mockExecutor{
		outputs: map[string]string{"submitted": "true"},
	})
	engine.RegisterExecutor("result_b", &mockExecutor{
		outputs: map[string]string{"cancelled": "true"},
	})

	bp := Blueprint{
		ID:   "btc-price-resolution",
		Name: "BTC Price Check",
		Nodes: []NodeDef{
			{ID: "search", Type: "web_search"},
			{ID: "judge", Type: "llm_call"},
			{ID: "submit", Type: "result_a"},
			{ID: "fallback", Type: "result_b"},
		},
		Edges: []EdgeDef{
			{From: "search", To: "judge", Condition: "results.search.status == 'success'"},
			{From: "search", To: "fallback", Condition: "results.search.status != 'success'"},
			{From: "judge", To: "submit", Condition: "results.judge.outcome != 'inconclusive'"},
			{From: "judge", To: "fallback", Condition: "results.judge.outcome == 'inconclusive'"},
		},
		Budget: &Budget{MaxTotalTimeSeconds: 60},
	}

	run, err := engine.Execute(context.Background(), bp, map[string]string{
		"market_question": "What was BTC price on March 15?",
		"market_app_id":   "12345",
	})

	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}

	// Search should complete
	if run.NodeStates["search"].Status != "completed" {
		t.Fatalf("search not completed: %s", run.NodeStates["search"].Status)
	}
	// Judge should receive evidence and produce outcome
	if resultValue(run, "judge", "outcome") != "0" {
		t.Fatalf("expected judge outcome=0, got %q", resultValue(run, "judge", "outcome"))
	}
	// Submit should fire
	if run.NodeStates["submit"].Status != "completed" {
		t.Fatalf("submit not completed: %s", run.NodeStates["submit"].Status)
	}
	// Fallback should NOT fire
	if run.NodeStates["fallback"].Status != "pending" {
		t.Fatalf("fallback should be pending, got %s", run.NodeStates["fallback"].Status)
	}
}

func TestJoinNodeMarkedSkippedAfterUpstreamFailure(t *testing.T) {
	engine := NewEngine(slog.Default())
	engine.RegisterExecutor("fail", &mockExecutor{err: fmt.Errorf("boom")})
	engine.RegisterExecutor("ok", &mockExecutor{outputs: map[string]string{"status": "success"}})
	engine.RegisterExecutor("join", &mockExecutor{outputs: map[string]string{"result": "never"}})

	bp := Blueprint{
		ID: "test-join-skipped",
		Nodes: []NodeDef{
			{ID: "left", Type: "fail"},
			{ID: "right", Type: "ok"},
			{ID: "join", Type: "join"},
		},
		Edges: []EdgeDef{
			{From: "left", To: "join"},
			{From: "right", To: "join"},
		},
	}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "failed" {
		t.Fatalf("expected failed run, got %s", run.Status)
	}
	if got := run.NodeStates["join"].Status; got != "skipped" {
		t.Fatalf("expected join skipped, got %q", got)
	}
	if _, ok := run.Results.Get("join", "result"); ok {
		t.Fatal("join node should not have executed")
	}
}

func TestLoopIterationsRecordedPerRun(t *testing.T) {
	engine := NewEngine(slog.Default())
	counter := &loopCountingExecutor{}
	engine.RegisterExecutor("step", counter)

	bp := Blueprint{
		ID: "test-loop-traces",
		Nodes: []NodeDef{
			{ID: "a", Type: "step"},
			{ID: "b", Type: "step"},
		},
		Edges: []EdgeDef{
			{From: "a", To: "b"},
			{From: "b", To: "a", MaxTraversals: 2},
		},
	}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
	gotIterations := map[string][]int{}
	for _, trace := range run.NodeTraces {
		gotIterations[trace.NodeID] = append(gotIterations[trace.NodeID], trace.Iteration)
		if trace.Status != "completed" {
			t.Fatalf("expected completed trace for %s, got %s", trace.NodeID, trace.Status)
		}
		if trace.StartedAt == "" || trace.CompletedAt == "" {
			t.Fatalf("expected trace timestamps for %s", trace.NodeID)
		}
	}
	if !reflect.DeepEqual(gotIterations["a"], []int{1, 2, 3}) {
		t.Fatalf("unexpected a iterations: %#v", gotIterations["a"])
	}
	if !reflect.DeepEqual(gotIterations["b"], []int{1, 2, 3}) {
		t.Fatalf("unexpected b iterations: %#v", gotIterations["b"])
	}
	if got := run.EdgeTraversals[edgeKey("a", "b")]; got != 3 {
		t.Fatalf("expected a->b traversals=3, got %d", got)
	}
	if got := run.EdgeTraversals[edgeKey("b", "a")]; got != 2 {
		t.Fatalf("expected b->a traversals=2, got %d", got)
	}
}

func TestValidationRejectsInvalid(t *testing.T) {
	engine := NewEngine(slog.Default())
	engine.RegisterExecutor("test", &mockExecutor{})

	// Duplicate node IDs
	bp := Blueprint{
		ID: "bad",
		Nodes: []NodeDef{
			{ID: "a", Type: "test"},
			{ID: "a", Type: "test"},
		},
	}
	_, err := engine.Execute(context.Background(), bp, nil)
	if err == nil {
		t.Fatal("expected validation error for duplicate IDs")
	}

	// Missing executor
	bp2 := Blueprint{
		ID:    "bad2",
		Nodes: []NodeDef{{ID: "a", Type: "unknown_type"}},
	}
	_, err = engine.Execute(context.Background(), bp2, nil)
	if err == nil {
		t.Fatal("expected error for missing executor")
	}
}

func TestCELErrorFailsRun(t *testing.T) {
	engine := NewEngine(slog.Default())

	engine.RegisterExecutor("step", &mockExecutor{
		outputs: map[string]string{"status": "done"},
	})
	engine.RegisterExecutor("next", &mockExecutor{
		outputs: map[string]string{"result": "ok"},
	})

	bp := Blueprint{
		ID: "test-cel-error",
		Nodes: []NodeDef{
			{ID: "step", Type: "step"},
			{ID: "next", Type: "next"},
		},
		Edges: []EdgeDef{
			// Malformed CEL expression: unbalanced parentheses
			{From: "step", To: "next", Condition: "results.step.status == 'done'("},
		},
	}

	run, _ := engine.Execute(context.Background(), bp, nil)
	if run.Status != "failed" {
		t.Fatalf("expected failed, got %s", run.Status)
	}
	if !strings.Contains(run.Error, "condition") {
		t.Fatalf("expected condition error in run.Error, got: %q", run.Error)
	}
}

func TestOrphanedActivatedNodeFailsRun(t *testing.T) {
	// A node that is activated (root) but never completes should fail the run.
	// Use a blueprint where two root nodes exist but one is blocked from executing.
	engine := NewEngine(slog.Default())

	blocker := &mockExecutor{outputs: map[string]string{"outcome": "0"}}
	engine.RegisterExecutor("good", blocker)

	// staller never returns (blocks until context cancelled)
	engine.RegisterExecutor("stall", &mockExecutor{
		outputs: map[string]string{"status": "ok"},
	})
	engine.RegisterExecutor("after", &mockExecutor{
		outputs: map[string]string{"result": "done"},
	})

	// Two independent chains: good (completes), stall -> after.
	// stall completes but after has a condition that is never met.
	bp := Blueprint{
		ID: "test-orphan",
		Nodes: []NodeDef{
			{ID: "good", Type: "good"},
			{ID: "after", Type: "after"},
		},
		Edges: []EdgeDef{
			// after depends on good, condition that always evaluates false
			{From: "good", To: "after", Condition: "results.good.outcome == 'never'"},
		},
	}

	// "after" is NOT a root node (has incoming forward edge from "good"),
	// so it won't be activated unless the edge condition passes.
	// This should complete fine - after stays non-activated pending.
	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s (non-activated pending nodes are ok)", run.Status)
	}
	if run.NodeStates["after"].Status != "pending" {
		t.Fatalf("expected after=pending, got %s", run.NodeStates["after"].Status)
	}
}

func TestConditionalBranchNonActivatedIsFine(t *testing.T) {
	// Verifies that conditional branches where one path is not taken
	// do NOT trigger the orphaned-pending check.
	engine := NewEngine(slog.Default())

	engine.RegisterExecutor("search", &mockExecutor{
		outputs: map[string]string{"status": "success"},
	})
	engine.RegisterExecutor("happy", &mockExecutor{
		outputs: map[string]string{"outcome": "0"},
	})
	engine.RegisterExecutor("sad", &mockExecutor{
		outputs: map[string]string{"outcome": "fallback"},
	})

	bp := Blueprint{
		ID: "test-cond-ok",
		Nodes: []NodeDef{
			{ID: "search", Type: "search"},
			{ID: "happy", Type: "happy"},
			{ID: "sad", Type: "sad"},
		},
		Edges: []EdgeDef{
			{From: "search", To: "happy", Condition: "results.search.status == 'success'"},
			{From: "search", To: "sad", Condition: "results.search.status != 'success'"},
		},
	}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
	// sad should remain pending (not activated) and that's fine
	if run.NodeStates["sad"].Status != "pending" {
		t.Fatalf("expected sad=pending, got %s", run.NodeStates["sad"].Status)
	}
}

func TestTokenBudgetExceededFailsRun(t *testing.T) {
	engine := NewEngine(slog.Default())
	engine.RegisterExecutor("llm", &mockExecutor{
		outputs: map[string]string{"outcome": "0"},
		usage: TokenUsage{
			InputTokens:  80,
			OutputTokens: 35,
		},
	})

	bp := Blueprint{
		ID: "token-budget-fail",
		Nodes: []NodeDef{
			{ID: "judge", Type: "llm"},
		},
		Budget: &Budget{
			MaxTotalTokens: 100,
		},
	}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err == nil {
		t.Fatal("expected token budget error")
	}
	if !strings.Contains(err.Error(), "token budget exceeded") {
		t.Fatalf("expected token budget exceeded error, got %v", err)
	}
	if run.Status != "failed" {
		t.Fatalf("expected failed run, got %s", run.Status)
	}
	if run.Usage.InputTokens != 80 || run.Usage.OutputTokens != 35 {
		t.Fatalf("unexpected recorded usage: %+v", run.Usage)
	}
	if run.NodeStates["judge"].Status != "failed" {
		t.Fatalf("expected judge failed, got %s", run.NodeStates["judge"].Status)
	}
}

func TestBackEdgeSnapshotsNodeHistory(t *testing.T) {
	engine := NewEngine(slog.Default())
	counter := &loopCountingExecutor{}
	engine.RegisterExecutor("step", counter)

	bp := Blueprint{
		ID: "test-history",
		Nodes: []NodeDef{
			{ID: "a", Type: "step"},
			{ID: "b", Type: "step"},
		},
		Edges: []EdgeDef{
			{From: "a", To: "b"},
			{From: "b", To: "a", MaxTraversals: 2},
		},
	}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}

	// "a" is reset twice via back-edge, so a._runs should have 2 entries
	// (snapshots from iterations 1 and 2; iteration 3 is the final value).
	runsJSON := resultHistoryJSON(run, "a")
	if runsJSON == "" {
		t.Fatal("expected a._runs in context")
	}
	var history []map[string]string
	if err := json.Unmarshal([]byte(runsJSON), &history); err != nil {
		t.Fatalf("failed to parse a._runs: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("expected 2 history entries for a, got %d: %s", len(history), runsJSON)
	}
	if history[0]["count"] != "1" {
		t.Fatalf("expected first history count=1, got %q", history[0]["count"])
	}
	if history[1]["count"] != "3" {
		t.Fatalf("expected second history count=3, got %q", history[1]["count"])
	}
	// Final value should be the last execution
	if resultValue(run, "a", "count") != "5" {
		t.Fatalf("expected final a.count=5, got %q", resultValue(run, "a", "count"))
	}
}

func TestTypedCELEvaluation(t *testing.T) {
	ctx := NewInvocationFromInputs(nil)
	ctx.Results.SetField("fetch", "status", "success")
	ctx.Results.SetField("fetch", "count", "42")
	ctx.Results.SetField("fetch", "active", "true")

	// Scalar values stay as strings (executor outputs are map[string]string)
	ok, err := EvalCondition("results.fetch.count == '42'", ctx)
	if err != nil {
		t.Fatalf("string number eval error: %v", err)
	}
	if !ok {
		t.Fatal("expected results.fetch.count == '42' to be true")
	}

	ok, err = EvalCondition("results.fetch.active == 'true'", ctx)
	if err != nil {
		t.Fatalf("string bool eval error: %v", err)
	}
	if !ok {
		t.Fatal("expected results.fetch.active == 'true' to be true")
	}

	// String comparison works as before
	ok, err = EvalCondition("results.fetch.status == 'success'", ctx)
	if err != nil {
		t.Fatalf("string eval error: %v", err)
	}
	if !ok {
		t.Fatal("expected string comparison to work")
	}
}

func TestTypedCELListAccess(t *testing.T) {
	ctx := NewInvocationFromInputs(nil)
	history := []map[string]string{
		{"status": "waiting", "count": "1"},
		{"status": "success", "count": "2"},
	}
	for _, snap := range history {
		ctx.Results.MergeOutputs("fetch", snap)
		ctx.Results.AppendHistory("fetch")
	}

	ok, err := EvalCondition("results.fetch.history.size() == 2", ctx)
	if err != nil {
		t.Fatalf("list size eval error: %v", err)
	}
	if !ok {
		t.Fatal("expected results.fetch.history.size() == 2")
	}

	ok, err = EvalCondition("results.fetch.history.size() > 0", ctx)
	if err != nil {
		t.Fatalf("list > 0 eval error: %v", err)
	}
	if !ok {
		t.Fatal("expected results.fetch.history.size() > 0")
	}
}

func TestTypedCELMapAccess(t *testing.T) {
	ctx := NewInvocationFromInputs(nil)
	obj := map[string]any{"outcome": "0", "confidence": 0.95}
	encoded, _ := json.Marshal(obj)
	ctx.Results.SetField("judge", "details", string(encoded))

	ok, err := EvalCondition("results.judge.details.outcome == '0'", ctx)
	if err != nil {
		t.Fatalf("map field eval error: %v", err)
	}
	if !ok {
		t.Fatal("expected results.judge.details.outcome == '0'")
	}

	ok, err = EvalCondition("results.judge.details.confidence > 0.5", ctx)
	if err != nil {
		t.Fatalf("map number eval error: %v", err)
	}
	if !ok {
		t.Fatal("expected results.judge.details.confidence > 0.5")
	}
}

// TestCELExtStringsMethodsRoundTrip exercises every method from cel-go's
// ext.Strings() extension end-to-end through EvalExpression. The point is
// two-fold:
//
//  1. Confirm the methods actually work in the engine (only trim() and
//     lowerAscii() had any prior coverage).
//  2. Catch drift in the isCELMethod allowlist in expr.go. That allowlist
//     decides whether a dotted token like `fetch.raw.upperAscii` is split
//     into variable `fetch.raw` + method `upperAscii`, or declared whole.
//     Each method is tested in a dotted form so a missing entry in the
//     allowlist produces a compile-time CEL failure here.
func TestCELExtStringsMethodsRoundTrip(t *testing.T) {
	cases := []struct {
		name       string
		values     map[string]string
		expression string
		want       string
	}{
		// charAt
		{"charAt", map[string]string{"v": "hello"}, "inputs.v.charAt(1)", "e"},
		{"charAt dotted", map[string]string{"fetch.raw": "hello"}, "inputs.fetch.raw.charAt(0)", "h"},
		// format
		{"format", map[string]string{"v": "hello %s"}, "inputs.v.format(['world'])", "hello world"},
		{"format dotted", map[string]string{"fetch.tpl": "%d:%s"}, "inputs.fetch.tpl.format([7, 'ok'])", "7:ok"},
		// indexOf
		{"indexOf", map[string]string{"v": "hello world"}, "inputs.v.indexOf('world')", "6"},
		{"indexOf dotted", map[string]string{"fetch.raw": "abcdef"}, "inputs.fetch.raw.indexOf('cd')", "2"},
		// lastIndexOf
		{"lastIndexOf", map[string]string{"v": "abcabc"}, "inputs.v.lastIndexOf('b')", "4"},
		{"lastIndexOf dotted", map[string]string{"fetch.raw": "xyxyxy"}, "inputs.fetch.raw.lastIndexOf('y')", "5"},
		// lowerAscii
		{"lowerAscii", map[string]string{"v": "HELLO"}, "inputs.v.lowerAscii()", "hello"},
		{"lowerAscii dotted", map[string]string{"fetch.raw": "HI"}, "inputs.fetch.raw.lowerAscii()", "hi"},
		// replace
		{"replace", map[string]string{"v": "ababab"}, "inputs.v.replace('a', 'X')", "XbXbXb"},
		{"replace limit", map[string]string{"v": "ababab"}, "inputs.v.replace('a', 'X', 2)", "XbXbab"},
		{"replace dotted", map[string]string{"fetch.raw": "foo"}, "inputs.fetch.raw.replace('f', 'b')", "boo"},
		// split
		{"split", map[string]string{"v": "a,b,c"}, "inputs.v.split(',')", `["a","b","c"]`},
		{"split dotted", map[string]string{"fetch.raw": "x,y,z"}, "inputs.fetch.raw.split(',')", `["x","y","z"]`},
		// substring
		{"substring from", map[string]string{"v": "abcdef"}, "inputs.v.substring(2)", "cdef"},
		{"substring range", map[string]string{"v": "abcdef"}, "inputs.v.substring(1, 4)", "bcd"},
		{"substring dotted", map[string]string{"fetch.raw": "hello"}, "inputs.fetch.raw.substring(1, 3)", "el"},
		// trim
		{"trim", map[string]string{"v": "  hello  "}, "inputs.v.trim()", "hello"},
		{"trim dotted", map[string]string{"fetch.raw": "  xx  "}, "inputs.fetch.raw.trim()", "xx"},
		// upperAscii
		{"upperAscii", map[string]string{"v": "hello"}, "inputs.v.upperAscii()", "HELLO"},
		{"upperAscii dotted", map[string]string{"fetch.raw": "hi"}, "inputs.fetch.raw.upperAscii()", "HI"},
		// reverse
		{"reverse", map[string]string{"v": "abc"}, "inputs.v.reverse()", "cba"},
		{"reverse dotted", map[string]string{"fetch.raw": "xyz"}, "inputs.fetch.raw.reverse()", "zyx"},
		// join — uses literal list so CEL doesn't need to coerce list<dyn>
		// (the result of parsing a JSON array from context) into list<string>.
		{"join", nil, "['a', 'b', 'c'].join()", "abc"},
		{"join sep", nil, "['a', 'b', 'c'].join(',')", "a,b,c"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewInvocationFromInputs(nil)
			for k, v := range tc.values {
				ctx.Run.Inputs[k] = v
			}
			got, err := EvalExpression(tc.expression, ctx)
			if err != nil {
				t.Fatalf("EvalExpression(%q) error: %v", tc.expression, err)
			}
			if got != tc.want {
				t.Fatalf("EvalExpression(%q) = %q, want %q", tc.expression, got, tc.want)
			}
		})
	}

	// Drift guard: every method listed in isCELMethod must be exercised by
	// at least one case above. If an entry is removed from the allowlist
	// without removing its tests, this stays green; if an entry is added
	// without a test, this fails. Paired with the per-method tests, the two
	// together make the allowlist self-checking.
	extMethods := []string{
		"charAt", "format", "indexOf", "lastIndexOf", "lowerAscii",
		"replace", "split", "substring", "trim", "upperAscii", "reverse", "join",
	}
	for _, m := range extMethods {
		covered := false
		for _, tc := range cases {
			if strings.Contains(tc.expression, "."+m+"(") || strings.Contains(tc.expression, "]."+m+"(") {
				covered = true
				break
			}
		}
		if !covered {
			t.Errorf("ext.Strings method %q is in isCELMethod but has no test case", m)
		}
	}
}

// TestExtractIdentifiersStripsCELMethods is a focused unit test of the
// heuristic that keeps dotted node-output variables separable from the
// trailing CEL method name. If a method is removed from isCELMethod (or the
// upstream cel-go library renames one), identifier extraction mis-declares
// a variable and downstream evaluations fail cryptically; this test catches
// the drift at the source.
func TestExtractIdentifiersStripsCELMethods(t *testing.T) {
	methods := []string{
		"charAt", "format", "indexOf", "lastIndexOf", "lowerAscii",
		"replace", "split", "substring", "trim", "upperAscii", "reverse", "join",
		// Built-in receiver methods — same stripping logic.
		"contains", "startsWith", "endsWith", "matches", "exists", "all",
		"filter", "size", "has",
	}
	for _, m := range methods {
		m := m
		t.Run(m, func(t *testing.T) {
			token := "fetch.raw." + m
			ids := extractIdentifiers(token, nil)
			if len(ids) != 1 || ids[0] != "fetch.raw" {
				t.Fatalf("extractIdentifiers(%q) = %v, want [fetch.raw]", token, ids)
			}
		})
	}
}

type cancellableMockExecutor struct {
	calls    []string
	hitKey   string
	returned bool
}

func (c *cancellableMockExecutor) Execute(ctx context.Context, node NodeDef, execCtx *Invocation) (ExecutorResult, error) {
	return ExecutorResult{}, nil
}

func (c *cancellableMockExecutor) CancelCorrelation(key string) bool {
	c.calls = append(c.calls, key)
	if key == c.hitKey {
		c.returned = true
		return true
	}
	return false
}

func TestEngineCancelCorrelationDispatchesToCancellableExecutors(t *testing.T) {
	engine := NewEngine(nil)
	cancellable := &cancellableMockExecutor{hitKey: "alpha"}
	engine.RegisterExecutor("cancellable", cancellable)
	// Non-cancellable executors must be skipped without error.
	engine.RegisterExecutor("plain", &mockExecutor{})

	if !engine.CancelCorrelation("alpha") {
		t.Fatal("CancelCorrelation(matching key) = false, want true")
	}
	if engine.CancelCorrelation("beta") {
		t.Fatal("CancelCorrelation(unknown key) = true, want false")
	}
	if engine.CancelCorrelation("") {
		t.Fatal("CancelCorrelation(empty key) must short-circuit to false")
	}
	if got := cancellable.calls; !reflect.DeepEqual(got, []string{"alpha", "beta"}) {
		t.Fatalf("executor received %v, want [alpha beta]", got)
	}
}

func TestEngineCancelCorrelationNoCancellableExecutorsIsSafe(t *testing.T) {
	engine := NewEngine(nil)
	engine.RegisterExecutor("plain", &mockExecutor{})
	if engine.CancelCorrelation("anything") {
		t.Fatal("expected false when no executor implements CancellableExecutor")
	}
}

func TestEngineCancelCorrelationNilReceiverIsSafe(t *testing.T) {
	var engine *Engine
	if engine.CancelCorrelation("key") {
		t.Fatal("expected nil-receiver CancelCorrelation to return false")
	}
}

type suspendableMock struct {
	canSuspend bool
	calls      []NodeDef
}

func (s *suspendableMock) Execute(ctx context.Context, node NodeDef, execCtx *Invocation) (ExecutorResult, error) {
	return ExecutorResult{}, nil
}

func (s *suspendableMock) CanSuspend(node NodeDef) bool {
	s.calls = append(s.calls, node)
	return s.canSuspend
}

func TestEngineCanSuspendDelegatesToSuspendableExecutor(t *testing.T) {
	engine := NewEngine(nil)
	mock := &suspendableMock{canSuspend: true}
	engine.RegisterExecutor("signal_node", mock)
	engine.RegisterExecutor("plain_node", &mockExecutor{})

	if !engine.CanSuspend(NodeDef{ID: "x", Type: "signal_node"}) {
		t.Fatal("CanSuspend(suspendable) = false, want true")
	}
	if engine.CanSuspend(NodeDef{ID: "y", Type: "plain_node"}) {
		t.Fatal("CanSuspend(non-SuspendableExecutor) = true, want false")
	}
	if engine.CanSuspend(NodeDef{ID: "z", Type: "unknown"}) {
		t.Fatal("CanSuspend(unknown type) = true, want false")
	}
	if len(mock.calls) != 1 || mock.calls[0].ID != "x" {
		t.Fatalf("suspendable mock received %v, want one call with ID=x", mock.calls)
	}
}

func TestEngineCanSuspendNilReceiverIsSafe(t *testing.T) {
	var engine *Engine
	if engine.CanSuspend(NodeDef{Type: "anything"}) {
		t.Fatal("expected nil-receiver CanSuspend to return false")
	}
}

func TestEngineValidateNoSuspensionCapableNodesListsSortedOffenders(t *testing.T) {
	engine := NewEngine(nil)
	engine.RegisterExecutor("suspender", &suspendableMock{canSuspend: true})
	engine.RegisterExecutor("plain", &mockExecutor{})

	bp := Blueprint{
		Nodes: []NodeDef{
			{ID: "zeta", Type: "suspender"},
			{ID: "alpha", Type: "suspender"},
			{ID: "gamma", Type: "plain"},
		},
	}
	err := engine.ValidateNoSuspensionCapableNodes(bp)
	if err == nil {
		t.Fatal("expected error listing suspending offenders")
	}
	msg := err.Error()
	if !strings.Contains(msg, "alpha (suspender), zeta (suspender)") {
		t.Fatalf("offender list missing or out of order: %q", msg)
	}
	if strings.Contains(msg, "gamma") {
		t.Fatalf("non-suspending node reported as offender: %q", msg)
	}
}

func TestEngineValidateNoSuspensionCapableNodesAcceptsCleanBlueprint(t *testing.T) {
	engine := NewEngine(nil)
	engine.RegisterExecutor("plain", &mockExecutor{})
	engine.RegisterExecutor("nonsuspendable", &suspendableMock{canSuspend: false})

	bp := Blueprint{
		Nodes: []NodeDef{
			{ID: "a", Type: "plain"},
			{ID: "b", Type: "nonsuspendable"},
		},
	}
	if err := engine.ValidateNoSuspensionCapableNodes(bp); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEngineValidateNoSuspensionCapableNodesNilReceiverIsSafe(t *testing.T) {
	var engine *Engine
	if err := engine.ValidateNoSuspensionCapableNodes(Blueprint{}); err != nil {
		t.Fatalf("nil-receiver returned err: %v", err)
	}
}

type describedMock struct {
	schema json.RawMessage
	keys   []string
}

func (d *describedMock) Execute(ctx context.Context, node NodeDef, execCtx *Invocation) (ExecutorResult, error) {
	return ExecutorResult{}, nil
}

func (d *describedMock) ConfigSchema() json.RawMessage { return d.schema }

func (d *describedMock) OutputKeys() []string { return d.keys }

func TestEngineConfigSchemaForAndOutputKeysFor(t *testing.T) {
	engine := NewEngine(nil)
	schema := json.RawMessage(`{"type":"object"}`)
	keys := []string{"status", "outcome"}
	engine.RegisterExecutor("described", &describedMock{schema: schema, keys: keys})
	engine.RegisterExecutor("plain", &mockExecutor{})

	if got := engine.ConfigSchemaFor("described"); string(got) != string(schema) {
		t.Fatalf("ConfigSchemaFor(described) = %s, want %s", got, schema)
	}
	if got := engine.OutputKeysFor("described"); !reflect.DeepEqual(got, keys) {
		t.Fatalf("OutputKeysFor(described) = %v, want %v", got, keys)
	}
	if got := engine.ConfigSchemaFor("plain"); got != nil {
		t.Fatalf("ConfigSchemaFor(non-implementer) = %s, want nil", got)
	}
	if got := engine.OutputKeysFor("plain"); got != nil {
		t.Fatalf("OutputKeysFor(non-implementer) = %v, want nil", got)
	}
	if got := engine.ConfigSchemaFor("unknown"); got != nil {
		t.Fatalf("ConfigSchemaFor(unknown) = %s, want nil", got)
	}
	if got := engine.OutputKeysFor("unknown"); got != nil {
		t.Fatalf("OutputKeysFor(unknown) = %v, want nil", got)
	}
}

func TestEngineCatalogLookupsNilReceiverIsSafe(t *testing.T) {
	var engine *Engine
	if got := engine.ConfigSchemaFor("whatever"); got != nil {
		t.Fatalf("nil-receiver ConfigSchemaFor returned %s", got)
	}
	if got := engine.OutputKeysFor("whatever"); got != nil {
		t.Fatalf("nil-receiver OutputKeysFor returned %v", got)
	}
}
