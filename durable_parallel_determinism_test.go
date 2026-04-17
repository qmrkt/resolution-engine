package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qmrkt/resolution-engine/dag"
	"pgregory.net/rapid"
)

// jitterExecutor sleeps a varying micro-duration per call so sibling
// goroutines finish in non-deterministic order. The sleep is seeded by an
// atomic counter so every invocation produces a different amount without
// needing a shared RNG. Used by the determinism property test below.
type jitterExecutor struct {
	counter atomic.Uint64
}

func (e *jitterExecutor) Execute(ctx context.Context, node dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	// Cheap pseudo-jitter: 0–700µs based on counter. Enough to reorder
	// goroutine completion in practice without materially slowing the
	// test.
	jitterMicros := time.Duration(e.counter.Add(1)%8) * 100 * time.Microsecond
	select {
	case <-time.After(jitterMicros):
		return dag.ExecutorResult{Outputs: map[string]string{
			"status": "success",
			"node":   node.ID,
		}}, nil
	case <-ctx.Done():
		return dag.ExecutorResult{}, ctx.Err()
	}
}

// runDeterminismSignature hashes the fields that must be identical across
// replays of the same blueprint: terminal run status, the final NodeStates
// map (order-independent), the ordered NodeTraces (order IS load-bearing —
// it encodes the apply sequence), and the final EdgeTraversals.
//
// Anything not on this list is allowed to vary between replays (wall-clock
// timestamps, runtime-dependent metadata).
func runDeterminismSignature(run *dag.RunState) string {
	if run == nil {
		return "<nil>"
	}
	h := sha256.New()
	fmt.Fprintf(h, "status=%s\n", run.Status)

	nodeIDs := make([]string, 0, len(run.NodeStates))
	for id := range run.NodeStates {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Strings(nodeIDs)
	for _, id := range nodeIDs {
		s := run.NodeStates[id]
		fmt.Fprintf(h, "state|%s|%s|%s|%d|%d\n", id, s.Status, s.Error, s.Usage.InputTokens, s.Usage.OutputTokens)
	}

	for i, tr := range run.NodeTraces {
		outputKeys := make([]string, 0, len(tr.Outputs))
		for k := range tr.Outputs {
			outputKeys = append(outputKeys, k)
		}
		sort.Strings(outputKeys)
		fmt.Fprintf(h, "trace|%d|%s|%s|%d|%s|%s\n", i, tr.NodeID, tr.NodeType, tr.Iteration, tr.Status, tr.Error)
		for _, k := range outputKeys {
			fmt.Fprintf(h, "  out|%s=%s\n", k, tr.Outputs[k])
		}
	}

	edgeKeys := make([]string, 0, len(run.EdgeTraversals))
	for k := range run.EdgeTraversals {
		edgeKeys = append(edgeKeys, k)
	}
	sort.Strings(edgeKeys)
	for _, k := range edgeKeys {
		fmt.Fprintf(h, "edge|%s=%d\n", k, run.EdgeTraversals[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// fanoutBlueprintBytes builds a root → N siblings shape with an optional
// join tail. Uses raw strings everywhere (no interpolation) so the test
// doesn't fight the WIP migration to namespaced lookups.
func fanoutBlueprintBytes(width int, withJoin bool) []byte {
	nodes := []dag.NodeDef{
		{ID: "root", Type: "root", Config: map[string]interface{}{}},
	}
	edges := make([]dag.EdgeDef, 0, 2*width)
	for i := 0; i < width; i++ {
		id := "s" + strconv.Itoa(i)
		nodes = append(nodes, dag.NodeDef{ID: id, Type: "sibling", Config: map[string]interface{}{}})
		edges = append(edges, dag.EdgeDef{From: "root", To: id})
	}
	if withJoin {
		nodes = append(nodes, dag.NodeDef{ID: "join", Type: "root", Config: map[string]interface{}{}})
		for i := 0; i < width; i++ {
			edges = append(edges, dag.EdgeDef{From: "s" + strconv.Itoa(i), To: "join"})
		}
	}
	return mustMarshalBlueprint(dag.Blueprint{ID: "fanout-prop", Version: 1, Nodes: nodes, Edges: edges})
}

// TestDurableBatchApplyIsDeterministic is a property test: for any parallel
// fanout shape, replaying the same blueprint with an order-scrambling
// executor produces byte-identical final run state. Catches ordering bugs
// in applyBatchExecutions where outcomes would get applied in
// goroutine-completion order instead of the deterministic ready-slice
// order.
//
// Tune runtime with rapid's flag: `go test -rapid.checks=50` caps iterations
// at 50 (~21s under -race on an M-series laptop). Default is 100 (~40s). The
// -short skip below is for dev-loop speed; CI should drop -short and use
// -rapid.checks to bound wall time.
func TestDurableBatchApplyIsDeterministic(t *testing.T) {
	if testing.Short() {
		t.Skip("property test; run without -short (optionally -rapid.checks=50) for full coverage")
	}
	rapid.Check(t, func(rt *rapid.T) {
		width := rapid.IntRange(2, 6).Draw(rt, "fanoutWidth")
		withJoin := rapid.Bool().Draw(rt, "withJoin")
		bp := fanoutBlueprintBytes(width, withJoin)

		const trials = 4
		signatures := make([]string, trials)
		for trial := 0; trial < trials; trial++ {
			tmpDir := t.TempDir()
			root := &durableStaticExecutor{outputs: map[string]string{"status": "success"}}
			siblings := &jitterExecutor{}

			manager := newDurableTestManagerWithConfig(t, tmpDir, durableTestEngine(map[string]dag.Executor{
				"root":    root,
				"sibling": siblings,
			}), DurableRunManagerConfig{
				MaxWorkers:   1,
				MaxQueueSize: 4,
				PollInterval: 5 * time.Millisecond,
			})

			runID := fmt.Sprintf("det-%d-%d", width, trial)
			appID := 20000 + trial*10 + width
			if _, err := manager.Submit(RunRequest{RunID: runID, AppID: appID, BlueprintJSON: bp}); err != nil {
				manager.Close()
				t.Fatal(err)
			}
			result := waitForDurableTerminal(t, manager, runID)
			if result.Status != RunStatusCompleted {
				manager.Close()
				t.Fatalf("trial %d status = %q, want %q", trial, result.Status, RunStatusCompleted)
			}
			signatures[trial] = runDeterminismSignature(result.RunState)
			manager.Close()
		}
		for i := 1; i < trials; i++ {
			if signatures[i] != signatures[0] {
				t.Fatalf("trial %d signature diverged from baseline (width=%d, join=%v):\n  [0]: %s\n  [%d]: %s",
					i, width, withJoin, signatures[0], i, signatures[i])
			}
		}
	})
}
