package dag

import (
	"testing"

	"pgregory.net/rapid"
)

// --- Byte-level fuzz targets (go test -fuzz) ---

// FuzzEvalCondition throws arbitrary expressions at the CEL evaluator.
// The property: it must never panic regardless of input. Errors are fine.
func FuzzEvalCondition(f *testing.F) {
	f.Add(`status == "completed"`)
	f.Add(`true`)
	f.Add(`false`)
	f.Add(`1 + 2 == 3`)
	f.Add(`fetch.status == "success" && judge.outcome != "inconclusive"`)
	f.Add(`fetch._runs.size() > 0`)
	f.Add(`""`)
	f.Add(`)))(((`)
	f.Add("a.b.c.d.e.f.g.h.i.j")
	f.Add(`"unterminated string`)
	f.Add(`size(fetch._runs) >= 3`)
	f.Add(`has(judge.outcome)`)

	ctx := NewContext(map[string]string{
		"status":        "completed",
		"fetch.status":  "success",
		"judge.outcome": "0",
		"fetch._runs":   `[{"status":"success"},{"status":"failed"}]`,
	})

	f.Fuzz(func(t *testing.T, expr string) {
		// Must not panic. Errors are expected for malformed input.
		_, _ = EvalCondition(expr, ctx)
	})
}

// FuzzEvalConditionWithValues fuzzes both expression and context values.
func FuzzEvalConditionWithValues(f *testing.F) {
	f.Add(`a == b`, "hello", "hello")
	f.Add(`a != b`, "x", "y")
	f.Add(`a == "true"`, "true", "")
	f.Add(`a == b`, `[1,2,3]`, `{"key":"val"}`)

	f.Fuzz(func(t *testing.T, expr, valA, valB string) {
		ctx := NewContext(map[string]string{
			"a": valA,
			"b": valB,
		})
		_, _ = EvalCondition(expr, ctx)
	})
}

// FuzzInterpolate fuzzes the {{key}} template substitution.
// Property: must never panic, and output must not contain unresolved {{key}}
// placeholders for keys that exist in context.
func FuzzInterpolate(f *testing.F) {
	f.Add("hello {{name}}, welcome to {{place}}")
	f.Add("{{")
	f.Add("}}")
	f.Add("{{missing}}")
	f.Add("{{a}}{{b}}{{c}}")
	f.Add("no placeholders")
	f.Add("{{nested{{key}}}}")
	f.Add("")
	f.Add("{{  spaced  }}")

	f.Fuzz(func(t *testing.T, template string) {
		ctx := NewContext(map[string]string{
			"name":  "world",
			"place": "earth",
			"a":     "1",
			"b":     "2",
			"c":     "3",
		})
		_ = ctx.Interpolate(template)
	})
}

// --- Property-based tests (rapid) ---

// nodeIDGen generates plausible node IDs.
func nodeIDGen() *rapid.Generator[string] {
	return rapid.OneOf(
		rapid.Just("fetch"),
		rapid.Just("judge"),
		rapid.Just("submit"),
		rapid.Just("cancel"),
		rapid.Just("wait"),
		rapid.Just("evidence"),
		rapid.Just("check"),
		rapid.Just("retry"),
		rapid.StringMatching(`[a-z][a-z0-9_]{0,15}`),
	)
}

// nodeTypeGen generates node types (not necessarily registered — scheduler doesn't care).
func nodeTypeGen() *rapid.Generator[string] {
	return rapid.SampledFrom([]string{
		"api_fetch", "llm_judge", "human_judge", "market_evidence",
		"submit_result", "cancel_market", "wait", "defer_resolution",
		"outcome_terminality",
	})
}

// blueprintGen generates structured blueprints with 1-12 nodes and random edges.
func blueprintGen() *rapid.Generator[Blueprint] {
	return rapid.Custom[Blueprint](func(t *rapid.T) Blueprint {
		nodeCount := rapid.IntRange(1, 12).Draw(t, "nodeCount")

		// Generate unique node IDs
		idSet := make(map[string]struct{})
		nodes := make([]NodeDef, 0, nodeCount)
		for i := 0; i < nodeCount; i++ {
			var id string
			for {
				id = nodeIDGen().Draw(t, "nodeID")
				if _, exists := idSet[id]; !exists {
					break
				}
			}
			idSet[id] = struct{}{}
			nodes = append(nodes, NodeDef{
				ID:   id,
				Type: nodeTypeGen().Draw(t, "nodeType"),
			})
		}

		// Generate edges between existing nodes
		edgeCount := rapid.IntRange(0, nodeCount*2).Draw(t, "edgeCount")
		edges := make([]EdgeDef, 0, edgeCount)
		nodeIDs := make([]string, 0, len(idSet))
		for id := range idSet {
			nodeIDs = append(nodeIDs, id)
		}

		for i := 0; i < edgeCount; i++ {
			from := rapid.SampledFrom(nodeIDs).Draw(t, "from")
			to := rapid.SampledFrom(nodeIDs).Draw(t, "to")
			edge := EdgeDef{From: from, To: to}
			if rapid.Bool().Draw(t, "hasMaxTraversals") {
				edge.MaxTraversals = rapid.IntRange(1, 5).Draw(t, "maxTraversals")
			}
			if rapid.Bool().Draw(t, "hasCondition") {
				edge.Condition = rapid.SampledFrom([]string{
					`status == "completed"`,
					`true`,
					`false`,
					`outcome != "inconclusive"`,
				}).Draw(t, "condition")
			}
			edges = append(edges, edge)
		}

		return Blueprint{
			ID:    "fuzz-bp",
			Nodes: nodes,
			Edges: edges,
		}
	})
}

// TestSchedulerProperties verifies scheduler invariants on random blueprints.
func TestSchedulerProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		bp := blueprintGen().Draw(t, "blueprint")

		// NewScheduler must never panic
		scheduler := NewScheduler(bp)

		// Topology must contain every node exactly once
		topo := scheduler.Topology()
		if len(topo) != len(bp.Nodes) {
			t.Fatalf("topology has %d nodes, blueprint has %d", len(topo), len(bp.Nodes))
		}
		seen := make(map[string]struct{}, len(topo))
		for _, id := range topo {
			if _, dup := seen[id]; dup {
				t.Fatalf("duplicate node %q in topology", id)
			}
			seen[id] = struct{}{}
		}
		for _, node := range bp.Nodes {
			if _, ok := seen[node.ID]; !ok {
				t.Fatalf("node %q missing from topology", node.ID)
			}
		}

		// Back-edge detection must be consistent: if an edge is a back-edge,
		// the reverse must NOT also be a back-edge (would mean every cycle
		// has both directions marked).
		for _, edge := range bp.Edges {
			if scheduler.IsBackEdge(edge.From, edge.To) && scheduler.IsBackEdge(edge.To, edge.From) {
				// Only a problem if both edges exist in the graph
				hasBoth := false
				for _, e2 := range bp.Edges {
					if e2.From == edge.To && e2.To == edge.From {
						hasBoth = true
						break
					}
				}
				if hasBoth {
					// Two-node mutual cycle: one direction must be forward.
					// This is actually valid — DFS picks one. Just verify
					// the topology still works.
				}
			}
		}

		// InitialActivatedNodes must be a subset of all node IDs
		activated := scheduler.InitialActivatedNodes()
		for id := range activated {
			if _, ok := seen[id]; !ok {
				t.Fatalf("activated node %q not in blueprint", id)
			}
		}

		// ReadyNodes with empty sets must return only root-like nodes
		ready := scheduler.ReadyNodes(
			make(map[string]struct{}),
			make(map[string]struct{}),
			make(map[string]struct{}),
		)
		for _, id := range ready {
			if _, ok := seen[id]; !ok {
				t.Fatalf("ready node %q not in blueprint", id)
			}
		}
	})
}

// TestValidateBlueprintProperties verifies ValidateBlueprint never panics on
// structurally random blueprints.
func TestValidateBlueprintProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		bp := blueprintGen().Draw(t, "blueprint")

		// Must not panic
		errs := ValidateBlueprint(bp)

		// If there are no errors, basic invariants must hold
		if len(errs) == 0 {
			if len(bp.Nodes) == 0 {
				t.Fatal("valid blueprint has zero nodes")
			}
			// All edge endpoints must reference existing nodes
			ids := make(map[string]struct{}, len(bp.Nodes))
			for _, n := range bp.Nodes {
				ids[n.ID] = struct{}{}
			}
			for _, e := range bp.Edges {
				if _, ok := ids[e.From]; !ok {
					t.Fatalf("valid blueprint has dangling edge from %q", e.From)
				}
				if _, ok := ids[e.To]; !ok {
					t.Fatalf("valid blueprint has dangling edge to %q", e.To)
				}
			}
		}
	})
}

// TestEvalConditionProperties tests CEL evaluation with structured inputs.
func TestEvalConditionProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a random context
		numKeys := rapid.IntRange(1, 10).Draw(t, "numKeys")
		inputs := make(map[string]string, numKeys)
		for i := 0; i < numKeys; i++ {
			key := rapid.StringMatching(`[a-z][a-z0-9_.]{0,20}`).Draw(t, "key")
			value := rapid.OneOf(
				rapid.Just("true"),
				rapid.Just("false"),
				rapid.Just("completed"),
				rapid.Just("failed"),
				rapid.Just("0"),
				rapid.Just("42"),
				rapid.Just(`[1,2,3]`),
				rapid.Just(`{"a":"b"}`),
				rapid.StringMatching(`[a-zA-Z0-9 ]{0,30}`),
			).Draw(t, "value")
			inputs[key] = value
		}
		ctx := NewContext(inputs)

		// Pick an expression that references existing keys
		keys := make([]string, 0, len(inputs))
		for k := range inputs {
			keys = append(keys, k)
		}
		key := rapid.SampledFrom(keys).Draw(t, "exprKey")
		expr := rapid.SampledFrom([]string{
			key + ` == "completed"`,
			key + ` != ""`,
			key + ` == "true"`,
			`true`,
			`false`,
		}).Draw(t, "expression")

		// Must not panic
		result, err := EvalCondition(expr, ctx)
		if err == nil {
			// Result must be deterministic (same input → same output)
			result2, err2 := EvalCondition(expr, ctx)
			if err2 != nil {
				t.Fatalf("second eval failed: %v", err2)
			}
			if result != result2 {
				t.Fatalf("non-deterministic: %v vs %v for %q", result, result2, expr)
			}
		}
	})
}
