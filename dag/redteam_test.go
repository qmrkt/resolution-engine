package dag

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 1. Malformed DAGs
// ---------------------------------------------------------------------------

func TestMalformedEmptyBlueprint(t *testing.T) {
	engine := NewEngine(slog.Default())
	bp := Blueprint{ID: "empty", Nodes: nil}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("empty blueprint should complete, got %s", run.Status)
	}
}

func TestMalformedDuplicateNodeIDs(t *testing.T) {
	engine := NewEngine(slog.Default())
	engine.RegisterExecutor("test", &mockExecutor{})

	bp := Blueprint{
		ID: "dupes",
		Nodes: []NodeDef{
			{ID: "a", Type: "test"},
			{ID: "a", Type: "test"},
		},
	}

	_, err := engine.Execute(context.Background(), bp, nil)
	if err == nil {
		t.Fatal("expected validation error for duplicate IDs")
	}
}

func TestMalformedEdgeToNonexistentNode(t *testing.T) {
	engine := NewEngine(slog.Default())
	engine.RegisterExecutor("test", &mockExecutor{})

	bp := Blueprint{
		ID:    "bad-edge",
		Nodes: []NodeDef{{ID: "a", Type: "test"}},
		Edges: []EdgeDef{{From: "a", To: "nonexistent"}},
	}

	_, err := engine.Execute(context.Background(), bp, nil)
	if err == nil {
		t.Fatal("expected validation error for bad edge")
	}
}

func TestMalformedNoExecutorRegistered(t *testing.T) {
	engine := NewEngine(slog.Default())

	bp := Blueprint{
		ID:    "missing-executor",
		Nodes: []NodeDef{{ID: "a", Type: "invented_type"}},
	}

	_, err := engine.Execute(context.Background(), bp, nil)
	if err == nil {
		t.Fatal("expected error for missing executor")
	}
	if !strings.Contains(err.Error(), "no executor") {
		t.Fatalf("unexpected error: %s", err)
	}
}

// ---------------------------------------------------------------------------
// 2. Cycle/Loop attacks
// ---------------------------------------------------------------------------

func TestCycleWithoutMaxTraversals(t *testing.T) {
	// A→B→A cycle with no max_traversals should not run forever.
	// Back-edges without max_traversals are treated as exhausted (0 limit).
	engine := NewEngine(slog.Default())
	engine.RegisterExecutor("step", &mockExecutor{outputs: map[string]string{"done": "yes"}})

	bp := Blueprint{
		ID: "cycle",
		Nodes: []NodeDef{
			{ID: "a", Type: "step"},
			{ID: "b", Type: "step"},
		},
		Edges: []EdgeDef{
			{From: "a", To: "b"},
			{From: "b", To: "a"}, // back-edge, no max_traversals → exhausted immediately
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	run, err := engine.Execute(ctx, bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Should complete — back-edge is exhausted immediately
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
}

func TestCycleWithMaxTraversals(t *testing.T) {
	// A→B→A loop with max_traversals=3 on the back-edge.
	counter := &countingExecutor{}
	engine := NewEngine(slog.Default())
	engine.RegisterExecutor("step", counter)

	bp := Blueprint{
		ID: "bounded-cycle",
		Nodes: []NodeDef{
			{ID: "a", Type: "step"},
			{ID: "b", Type: "step"},
		},
		Edges: []EdgeDef{
			{From: "a", To: "b"},
			{From: "b", To: "a", MaxTraversals: 3}, // back-edge
		},
	}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
	// A runs: 1 initial + 3 back-edge = 4, B runs: 4 times = 8 total
	if counter.count != 8 {
		t.Fatalf("expected 8 executions (4 per node), got %d", counter.count)
	}
}

type countingExecutor struct {
	count int
}

func (e *countingExecutor) Execute(ctx context.Context, node NodeDef, execCtx *Invocation) (ExecutorResult, error) {
	e.count++
	return ExecutorResult{Outputs: map[string]string{"count": fmt.Sprintf("%d", e.count)}}, nil
}

// ---------------------------------------------------------------------------
// 3. Budget bypass attempts
// ---------------------------------------------------------------------------

func TestZeroBudgetTimeout(t *testing.T) {
	// Budget with 0 timeout should not bypass — should still execute.
	engine := NewEngine(slog.Default())
	engine.RegisterExecutor("quick", &mockExecutor{outputs: map[string]string{"ok": "yes"}})

	bp := Blueprint{
		ID:     "zero-budget",
		Nodes:  []NodeDef{{ID: "a", Type: "quick"}},
		Budget: &Budget{MaxTotalTimeSeconds: 0}, // 0 = no timeout
	}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
}

func TestNegativeBudgetIgnored(t *testing.T) {
	engine := NewEngine(slog.Default())
	engine.RegisterExecutor("quick", &mockExecutor{outputs: map[string]string{"ok": "yes"}})

	bp := Blueprint{
		ID:     "negative-budget",
		Nodes:  []NodeDef{{ID: "a", Type: "quick"}},
		Budget: &Budget{MaxTotalTimeSeconds: -1}, // nonsensical
	}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
}

// ---------------------------------------------------------------------------
// 4. Executor crash resilience
// ---------------------------------------------------------------------------

func TestExecutorPanicRecovery(t *testing.T) {
	// If an executor panics, the engine should recover and mark the node as failed.
	engine := NewEngine(slog.Default())
	engine.RegisterExecutor("panicker", &panickingExecutor{})

	bp := Blueprint{
		ID:    "panic-test",
		Nodes: []NodeDef{{ID: "a", Type: "panicker"}},
	}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "failed" {
		t.Fatalf("expected failed, got %s", run.Status)
	}
	if run.NodeStates["a"].Status != "failed" {
		t.Fatalf("expected node a failed, got %s", run.NodeStates["a"].Status)
	}
	if !strings.Contains(run.NodeStates["a"].Error, "panicked") {
		t.Fatalf("expected panic error, got %q", run.NodeStates["a"].Error)
	}
}

type panickingExecutor struct{}

func (e *panickingExecutor) Execute(ctx context.Context, node NodeDef, execCtx *Invocation) (ExecutorResult, error) {
	panic("executor crashed!")
}

// ---------------------------------------------------------------------------
// 5. SSRF prevention (api_fetch URL validation)
// ---------------------------------------------------------------------------

func TestSSRFLocalhost(t *testing.T) {
	// Verify that localhost URLs are not automatically blocked at the DAG level.
	// SSRF prevention should happen at the executor level, not the engine.
	// This test documents the current behavior.
	engine := NewEngine(slog.Default())

	// The DAG engine itself doesn't validate URLs — that's the executor's job.
	// We verify the engine accepts the blueprint (it's structurally valid).
	engine.RegisterExecutor("api_fetch", &mockExecutor{
		outputs: map[string]string{"status": "blocked"},
	})

	bp := Blueprint{
		ID: "ssrf-test",
		Nodes: []NodeDef{
			{
				ID:   "fetch",
				Type: "api_fetch",
				Config: map[string]interface{}{
					"url": "http://localhost:8080/admin",
				},
			},
		},
	}

	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Engine executes fine — SSRF blocking is executor responsibility
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
}

// ---------------------------------------------------------------------------
// 6. Large DAG stress test
// ---------------------------------------------------------------------------

func TestLargeDAG(t *testing.T) {
	engine := NewEngine(slog.Default())
	engine.RegisterExecutor("step", &mockExecutor{outputs: map[string]string{"ok": "yes"}})

	// Build a 16-node chain
	nodes := make([]NodeDef, 16)
	edges := make([]EdgeDef, 15)
	for i := 0; i < 16; i++ {
		nodes[i] = NodeDef{ID: fmt.Sprintf("n%d", i), Type: "step"}
		if i > 0 {
			edges[i-1] = EdgeDef{From: fmt.Sprintf("n%d", i-1), To: fmt.Sprintf("n%d", i)}
		}
	}

	bp := Blueprint{ID: "large", Nodes: nodes, Edges: edges}
	run, err := engine.Execute(context.Background(), bp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
	// All 16 nodes should complete
	for i := 0; i < 16; i++ {
		nid := fmt.Sprintf("n%d", i)
		if run.NodeStates[nid].Status != "completed" {
			t.Fatalf("node %s not completed: %s", nid, run.NodeStates[nid].Status)
		}
	}
}

// ---------------------------------------------------------------------------
// 7. Context injection attack
// ---------------------------------------------------------------------------

func TestContextInjectionViaInput(t *testing.T) {
	// Can an attacker inject context keys via inputs that override engine internals?
	engine := NewEngine(slog.Default())
	engine.RegisterExecutor("step", &mockExecutor{outputs: map[string]string{"result": "normal"}})

	bp := Blueprint{
		ID:    "inject",
		Nodes: []NodeDef{{ID: "step", Type: "step"}},
	}

	// Try to inject a fake "step.status" via inputs
	run, err := engine.Execute(context.Background(), bp, map[string]string{
		"step.status": "completed", // attacker tries to skip execution
		"step.result": "hacked",
	})
	if err != nil {
		t.Fatal(err)
	}
	// The engine should still execute the node and overwrite the injected values
	if run.NodeStates["step"].Status != "completed" {
		t.Fatalf("expected completed, got %s", run.NodeStates["step"].Status)
	}
	// The executor's output should override the injected value
	if resultValue(run, "step", "result") != "normal" {
		t.Fatalf("expected normal, got %q (injection succeeded!)", resultValue(run, "step", "result"))
	}
}
