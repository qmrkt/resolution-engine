package dag

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
)

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

var _ io.Writer = nopWriter{}

// --- Benchmark fixtures ---

func smallBlueprint() Blueprint {
	return Blueprint{
		ID: "bench-small",
		Nodes: []NodeDef{
			{ID: "fetch", Type: "api_fetch"},
			{ID: "judge", Type: "llm_call"},
			{ID: "submit", Type: "submit_result"},
		},
		Edges: []EdgeDef{
			{From: "fetch", To: "judge"},
			{From: "judge", To: "submit"},
		},
	}
}

func mediumBlueprint() Blueprint {
	nodes := make([]NodeDef, 10)
	edges := make([]EdgeDef, 0, 15)
	for i := 0; i < 10; i++ {
		nodes[i] = NodeDef{ID: fmt.Sprintf("n%d", i), Type: "step"}
		if i > 0 {
			edges = append(edges, EdgeDef{From: fmt.Sprintf("n%d", i-1), To: fmt.Sprintf("n%d", i)})
		}
	}
	// Add a branch
	edges = append(edges, EdgeDef{From: "n2", To: "n7", Condition: `n2.status == "completed"`})
	return Blueprint{ID: "bench-medium", Nodes: nodes, Edges: edges}
}

func largeBlueprint() Blueprint {
	nodes := make([]NodeDef, 16)
	edges := make([]EdgeDef, 0, 30)
	for i := 0; i < 16; i++ {
		nodes[i] = NodeDef{ID: fmt.Sprintf("n%d", i), Type: "step"}
	}
	// Chain + branches + back-edge
	for i := 0; i < 15; i++ {
		edges = append(edges, EdgeDef{From: fmt.Sprintf("n%d", i), To: fmt.Sprintf("n%d", i+1)})
	}
	for i := 0; i < 8; i++ {
		edges = append(edges, EdgeDef{From: fmt.Sprintf("n%d", i), To: fmt.Sprintf("n%d", i+8)})
	}
	edges = append(edges, EdgeDef{From: "n15", To: "n5", MaxTraversals: 3})
	return Blueprint{ID: "bench-large", Nodes: nodes, Edges: edges}
}

func contextWithKeys(n int) *Context {
	inputs := make(map[string]string, n)
	for i := 0; i < n; i++ {
		inputs[fmt.Sprintf("key_%d", i)] = fmt.Sprintf("value_%d", i)
	}
	return NewContext(inputs)
}

// --- Scheduler benchmarks ---

func BenchmarkNewScheduler_Small(b *testing.B) {
	bp := smallBlueprint()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewScheduler(bp)
	}
}

func BenchmarkNewScheduler_Medium(b *testing.B) {
	bp := mediumBlueprint()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewScheduler(bp)
	}
}

func BenchmarkNewScheduler_Large(b *testing.B) {
	bp := largeBlueprint()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewScheduler(bp)
	}
}

func BenchmarkReadyNodes_Small(b *testing.B) {
	s := NewScheduler(smallBlueprint())
	completed := make(map[string]struct{})
	failed := make(map[string]struct{})
	running := make(map[string]struct{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.ReadyNodes(completed, failed, running)
	}
}

func BenchmarkReadyNodes_Large(b *testing.B) {
	s := NewScheduler(largeBlueprint())
	completed := make(map[string]struct{})
	failed := make(map[string]struct{})
	running := make(map[string]struct{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.ReadyNodes(completed, failed, running)
	}
}

func BenchmarkEvaluateEdges(b *testing.B) {
	bp := mediumBlueprint()
	s := NewScheduler(bp)
	ctx := contextWithKeys(20)
	ctx.Set("n2.status", "completed")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.EvaluateEdges("n2", ctx)
	}
}

// --- CEL evaluation benchmarks ---

func BenchmarkEvalCondition_Simple(b *testing.B) {
	ctx := NewContext(map[string]string{"status": "completed"})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EvalCondition(`status == "completed"`, ctx)
	}
}

func BenchmarkEvalCondition_Complex(b *testing.B) {
	ctx := NewContext(map[string]string{
		"fetch.status":     "success",
		"judge.outcome":    "0",
		"judge.confidence": "high",
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EvalCondition(`fetch.status == "success" && judge.outcome != "inconclusive"`, ctx)
	}
}

func BenchmarkEvalCondition_WithList(b *testing.B) {
	ctx := NewContext(map[string]string{
		"fetch._runs": `[{"status":"success"},{"status":"failed"},{"status":"success"}]`,
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EvalCondition(`fetch._runs.size() >= 2`, ctx)
	}
}

// --- Context benchmarks ---

func BenchmarkContextSet(b *testing.B) {
	ctx := NewContext(nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.Set(fmt.Sprintf("key_%d", i%100), "value")
	}
}

func BenchmarkContextGet(b *testing.B) {
	ctx := contextWithKeys(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.Get(fmt.Sprintf("key_%d", i%100))
	}
}

func BenchmarkContextSnapshot_20(b *testing.B) {
	ctx := contextWithKeys(20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.Snapshot()
	}
}

func BenchmarkContextSnapshot_100(b *testing.B) {
	ctx := contextWithKeys(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.Snapshot()
	}
}

func BenchmarkContextInterpolate_Simple(b *testing.B) {
	ctx := NewContext(map[string]string{"name": "world"})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.Interpolate("hello {{name}}")
	}
}

func BenchmarkContextInterpolate_Multi(b *testing.B) {
	ctx := NewContext(map[string]string{
		"question": "Will BTC hit 100k?",
		"evidence": "Market data shows...",
		"outcomes": "[\"Yes\",\"No\"]",
	})
	template := "Question: {{question}}\nEvidence: {{evidence}}\nOutcomes: {{outcomes}}"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx.Interpolate(template)
	}
}

// --- Clone benchmarks ---

func BenchmarkCloneRunState_Small(b *testing.B) {
	bp := smallBlueprint()
	run := &RunState{
		ID:             "bench",
		Definition:     bp,
		NodeStates:     make(map[string]NodeState, 3),
		Context:        make(map[string]string, 20),
		Inputs:         make(map[string]string, 5),
		EdgeTraversals: make(map[string]int, 2),
	}
	for _, n := range bp.Nodes {
		run.NodeStates[n.ID] = NodeState{Status: "completed"}
	}
	for i := 0; i < 20; i++ {
		run.Context[fmt.Sprintf("k%d", i)] = fmt.Sprintf("v%d", i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cloneRunState(run)
	}
}

func BenchmarkCloneRunState_Large(b *testing.B) {
	bp := largeBlueprint()
	run := &RunState{
		ID:             "bench",
		Definition:     bp,
		NodeStates:     make(map[string]NodeState, 16),
		Context:        make(map[string]string, 200),
		Inputs:         make(map[string]string, 10),
		EdgeTraversals: make(map[string]int, 30),
		NodeTraces:     make([]NodeTrace, 16),
	}
	for _, n := range bp.Nodes {
		run.NodeStates[n.ID] = NodeState{Status: "completed"}
	}
	for i := 0; i < 200; i++ {
		run.Context[fmt.Sprintf("k%d", i)] = fmt.Sprintf("v%d", i)
	}
	for i := 0; i < 16; i++ {
		run.NodeTraces[i] = NodeTrace{
			NodeID:        fmt.Sprintf("n%d", i),
			InputSnapshot: make(map[string]string, 50),
			Outputs:       make(map[string]string, 10),
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cloneRunState(run)
	}
}

// --- Validation benchmarks ---

func BenchmarkValidateBlueprint_Small(b *testing.B) {
	bp := smallBlueprint()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ValidateBlueprint(bp)
	}
}

func BenchmarkValidateBlueprint_Large(b *testing.B) {
	bp := largeBlueprint()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ValidateBlueprint(bp)
	}
}

// --- Engine execution benchmark ---

type noopExecutor struct{}

func (e *noopExecutor) Execute(_ context.Context, _ NodeDef, _ *Context) (ExecutorResult, error) {
	return ExecutorResult{Outputs: map[string]string{"status": "success"}}, nil
}

func BenchmarkEngineExecute_3Nodes(b *testing.B) {
	engine := NewEngine(slog.New(slog.NewTextHandler(nopWriter{}, nil)))
	engine.RegisterExecutor("api_fetch", &noopExecutor{})
	engine.RegisterExecutor("llm_call", &noopExecutor{})
	engine.RegisterExecutor("submit_result", &noopExecutor{})
	bp := smallBlueprint()
	ctx := context.Background()
	inputs := map[string]string{"market_app_id": "1"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engine.Execute(ctx, bp, inputs)
	}
}

func BenchmarkEngineExecute_10Nodes(b *testing.B) {
	engine := NewEngine(slog.New(slog.NewTextHandler(nopWriter{}, nil)))
	engine.RegisterExecutor("step", &noopExecutor{})
	bp := mediumBlueprint()
	ctx := context.Background()
	inputs := map[string]string{"market_app_id": "1"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engine.Execute(ctx, bp, inputs)
	}
}
