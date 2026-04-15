package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/question-market/resolution-engine/dag"
	"pgregory.net/rapid"
)

// stubExecutor always succeeds with configurable outputs.
type stubExecutor struct {
	outputs map[string]string
}

func (e *stubExecutor) Execute(_ context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	outputs := make(map[string]string, len(e.outputs))
	for k, v := range e.outputs {
		outputs[k] = v
	}
	// Terminal nodes need specific outputs to satisfy result validation.
	switch node.Type {
	case "submit_result":
		outputs["status"] = "success"
		outputs["outcome"] = "0"
		outputs["evidence_hash"] = "abc123"
		outputs["submitted"] = "true"
	case "cancel_market":
		outputs["status"] = "success"
		outputs["cancelled"] = "true"
		outputs["reason"] = "test"
	case "defer_resolution":
		outputs["status"] = "success"
		outputs["deferred"] = "true"
		outputs["reason"] = "test"
	default:
		if outputs["status"] == "" {
			outputs["status"] = "success"
		}
	}
	return dag.ExecutorResult{Outputs: outputs}, nil
}

var terminalTypes = []string{"submit_result", "cancel_market", "defer_resolution"}

var nonTerminalTypes = []string{
	"api_fetch", "llm_call", "await_signal",
	"wait", "cel_eval",
}

var allTypes = append(append([]string{}, nonTerminalTypes...), terminalTypes...)

// blueprintGen generates structurally diverse blueprints for validation fuzzing.
func blueprintGen() *rapid.Generator[dag.Blueprint] {
	return rapid.Custom[dag.Blueprint](func(t *rapid.T) dag.Blueprint {
		nodeCount := rapid.IntRange(1, 12).Draw(t, "nodeCount")

		idPool := []string{
			"fetch", "judge", "submit", "cancel", "wait", "evidence",
			"check", "retry", "defer", "gate", "llm", "llm2",
		}

		// Pick unique IDs
		usedIDs := make(map[string]struct{})
		nodes := make([]dag.NodeDef, 0, nodeCount)
		for i := 0; i < nodeCount; i++ {
			var id string
			if i < len(idPool) {
				id = idPool[i]
			} else {
				id = rapid.StringMatching(`[a-z][a-z0-9]{1,8}`).Draw(t, "id")
			}
			if _, dup := usedIDs[id]; dup {
				continue
			}
			usedIDs[id] = struct{}{}

			nodeType := rapid.SampledFrom(allTypes).Draw(t, "type")
			// Ensure at least one terminal node
			if i == nodeCount-1 {
				hasTerminal := false
				for _, n := range nodes {
					for _, tt := range terminalTypes {
						if n.Type == tt {
							hasTerminal = true
						}
					}
				}
				if !hasTerminal {
					nodeType = rapid.SampledFrom(terminalTypes).Draw(t, "terminalType")
				}
			}

			node := dag.NodeDef{
				ID:   id,
				Type: nodeType,
			}

			// Add minimal valid config for node types that require it
			switch nodeType {
			case "api_fetch":
				node.Config = map[string]interface{}{
					"url": "https://example.com/api",
				}
			case "llm_call":
				node.Config = map[string]interface{}{
					"prompt": "Evaluate: {{evidence.raw}}",
				}
			case "await_signal":
				node.Config = map[string]interface{}{
					"signal_type":      "human_judgment.responded",
					"required_payload": []string{"outcome"},
					"default_outputs":  map[string]string{"status": "responded"},
					"timeout_seconds":  3600,
				}
			case "submit_result":
				node.Config = map[string]interface{}{
					"outcome_key": "judge.outcome",
				}
			case "wait":
				node.Config = map[string]interface{}{
					"duration_seconds": 0,
					"mode":             "sleep",
				}
			default:
				node.Config = map[string]interface{}{}
			}

			nodes = append(nodes, node)
		}

		// Build edge list
		nodeIDs := make([]string, 0, len(usedIDs))
		for id := range usedIDs {
			nodeIDs = append(nodeIDs, id)
		}

		edgeCount := rapid.IntRange(0, len(nodeIDs)*2).Draw(t, "edgeCount")
		edgeSet := make(map[string]struct{})
		edges := make([]dag.EdgeDef, 0, edgeCount)
		for i := 0; i < edgeCount; i++ {
			from := rapid.SampledFrom(nodeIDs).Draw(t, "from")
			to := rapid.SampledFrom(nodeIDs).Draw(t, "to")
			key := from + "->" + to
			if _, dup := edgeSet[key]; dup {
				continue
			}
			edgeSet[key] = struct{}{}

			edge := dag.EdgeDef{From: from, To: to}
			if rapid.Bool().Draw(t, "hasMaxTraversals") {
				edge.MaxTraversals = rapid.IntRange(1, 5).Draw(t, "maxTraversals")
			}
			edges = append(edges, edge)
		}

		return dag.Blueprint{
			ID:    "fuzz",
			Nodes: nodes,
			Edges: edges,
		}
	})
}

// TestValidateResolutionBlueprintProperties runs the full validation pipeline
// on random blueprints. The property: must never panic.
func TestValidateResolutionBlueprintProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		bp := blueprintGen().Draw(t, "blueprint")
		rawJSON, err := json.Marshal(bp)
		if err != nil {
			t.Fatal(err)
		}

		// Must not panic
		result := ValidateResolutionBlueprint(bp, rawJSON)

		// If valid, it must have zero issues
		if result.Valid && len(result.Issues) > 0 {
			t.Fatalf("valid=true but %d issues reported", len(result.Issues))
		}
		if !result.Valid && len(result.Issues) == 0 {
			t.Fatal("valid=false but no issues reported")
		}
	})
}

// TestEngineExecutesValidBlueprints verifies that blueprints passing validation
// can be executed by the engine without panics.
func TestEngineExecutesValidBlueprints(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		bp := blueprintGen().Draw(t, "blueprint")
		rawJSON, err := json.Marshal(bp)
		if err != nil {
			t.Fatal(err)
		}

		validation := ValidateResolutionBlueprint(bp, rawJSON)
		if !validation.Valid {
			return // skip invalid blueprints — we only test the engine on valid ones
		}

		// Wire up a stub executor for every type
		engine := dag.NewEngine(nil)
		stub := &stubExecutor{outputs: map[string]string{"outcome": "0"}}
		for _, nodeType := range allTypes {
			engine.RegisterExecutor(nodeType, stub)
		}

		ctx, cancel := context.WithTimeout(context.Background(), DefaultRunTimeout)
		defer cancel()

		// Must not panic. Errors are acceptable (e.g. orphaned nodes).
		run, _ := engine.Execute(ctx, bp, map[string]string{
			"market_app_id": "999",
		})

		if run != nil {
			// Run state must have an entry for every blueprint node
			for _, node := range bp.Nodes {
				if _, ok := run.NodeStates[node.ID]; !ok {
					t.Fatalf("run state missing node %q", node.ID)
				}
			}
		}
	})
}
