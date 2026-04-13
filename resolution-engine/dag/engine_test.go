package dag

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
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

func (m *mockExecutor) Execute(ctx context.Context, node NodeDef, execCtx *Context) (ExecutorResult, error) {
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	if m.err != nil {
		return ExecutorResult{}, m.err
	}
	return ExecutorResult{Outputs: m.outputs, Usage: m.usage}, nil
}

// contextAwareExecutor reads context during execution.
type contextAwareExecutor struct{}

func (e *contextAwareExecutor) Execute(ctx context.Context, node NodeDef, execCtx *Context) (ExecutorResult, error) {
	evidence := execCtx.Get("search.evidence")
	if evidence == "" {
		return ExecutorResult{Outputs: map[string]string{"outcome": "inconclusive"}}, nil
	}
	return ExecutorResult{Outputs: map[string]string{"outcome": "0"}}, nil
}

type runIDInspectingExecutor struct{}

func (e *runIDInspectingExecutor) Execute(ctx context.Context, node NodeDef, execCtx *Context) (ExecutorResult, error) {
	return ExecutorResult{
		Outputs: map[string]string{
			"resolution_run_id": execCtx.Get("resolution_run_id"),
			"input_run_id":      execCtx.Get("input.resolution_run_id"),
			"internal_run_id":   execCtx.Get("__run_id"),
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

func (e *loopCountingExecutor) Execute(ctx context.Context, node NodeDef, execCtx *Context) (ExecutorResult, error) {
	e.count++
	return ExecutorResult{Outputs: map[string]string{"count": fmt.Sprintf("%d", e.count)}}, nil
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
	if run.Context["a.result"] != "hello" {
		t.Fatalf("expected a.result=hello, got %q", run.Context["a.result"])
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
	if run.Inputs["resolution_run_id"] != run.ID {
		t.Fatalf("expected resolution_run_id=%q, got %q", run.ID, run.Inputs["resolution_run_id"])
	}
	if got := run.Context["step.resolution_run_id"]; got != run.ID {
		t.Fatalf("expected step.resolution_run_id=%q, got %q", run.ID, got)
	}
	if got := run.Context["step.input_run_id"]; got != run.ID {
		t.Fatalf("expected step.input_run_id=%q, got %q", run.ID, got)
	}
	if got := run.Context["step.internal_run_id"]; got != run.ID {
		t.Fatalf("expected step.internal_run_id=%q, got %q", run.ID, got)
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
	if run.Context["judge.outcome"] != "0" {
		t.Fatalf("expected judge.outcome=0, got %q", run.Context["judge.outcome"])
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
			{From: "search", To: "judge", Condition: "search.status == 'success'"},
			{From: "search", To: "fallback", Condition: "search.status != 'success'"},
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
			{From: "search", To: "judge", Condition: "search.status == 'success'"},
			{From: "search", To: "fallback", Condition: "search.status != 'success'"},
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
	if run.Context["fallback.outcome"] != "manual" {
		t.Fatalf("expected fallback.outcome=manual, got %q", run.Context["fallback.outcome"])
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

	engine.RegisterExecutor("work", &mockExecutor{
		outputs: map[string]string{"done": "yes"},
		delay:   100 * time.Millisecond,
	})

	bp := Blueprint{
		ID: "test-parallel",
		Nodes: []NodeDef{
			{ID: "a", Type: "work"},
			{ID: "b", Type: "work"},
			{ID: "c", Type: "work"},
		},
		// No edges — all three are roots and should run in parallel
	}

	start := time.Now()
	run, err := engine.Execute(context.Background(), bp, nil)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
	// If parallel, should complete in ~100ms, not ~300ms
	if elapsed > 250*time.Millisecond {
		t.Fatalf("expected parallel execution (<250ms), took %v", elapsed)
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
	ctx := NewContext(map[string]string{"question": "Will BTC hit 100k?"})

	result := ctx.Interpolate("Market: {{question}}")
	if result != "Market: Will BTC hit 100k?" {
		t.Fatalf("unexpected interpolation: %q", result)
	}

	result = ctx.Interpolate("No placeholders here")
	if result != "No placeholders here" {
		t.Fatalf("unexpected: %q", result)
	}

	result = ctx.Interpolate("Missing: {{unknown}}")
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
	engine.RegisterExecutor("llm_judge", &contextAwareExecutor{})
	engine.RegisterExecutor("submit_result", &mockExecutor{
		outputs: map[string]string{"submitted": "true"},
	})
	engine.RegisterExecutor("cancel_market", &mockExecutor{
		outputs: map[string]string{"cancelled": "true"},
	})

	bp := Blueprint{
		ID:   "btc-price-resolution",
		Name: "BTC Price Check",
		Nodes: []NodeDef{
			{ID: "search", Type: "web_search"},
			{ID: "judge", Type: "llm_judge"},
			{ID: "submit", Type: "submit_result"},
			{ID: "fallback", Type: "cancel_market"},
		},
		Edges: []EdgeDef{
			{From: "search", To: "judge", Condition: "search.status == 'success'"},
			{From: "search", To: "fallback", Condition: "search.status != 'success'"},
			{From: "judge", To: "submit", Condition: "judge.outcome != 'inconclusive'"},
			{From: "judge", To: "fallback", Condition: "judge.outcome == 'inconclusive'"},
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
	if run.Context["judge.outcome"] != "0" {
		t.Fatalf("expected judge outcome=0, got %q", run.Context["judge.outcome"])
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
	if _, ok := run.Context["join.result"]; ok {
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
			{From: "step", To: "next", Condition: "step.status == 'done'("},
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
			{From: "good", To: "after", Condition: "good.outcome == 'never'"},
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
			{From: "search", To: "happy", Condition: "search.status == 'success'"},
			{From: "search", To: "sad", Condition: "search.status != 'success'"},
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
