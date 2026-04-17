package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/qmrkt/resolution-engine/dag"
	"github.com/qmrkt/resolution-engine/executors"
)

var gadgetValidationAdapter = adaptBlueprintValidation

func mustBlueprintJSONMain(t *testing.T, bp dag.Blueprint) string {
	t.Helper()
	raw, err := json.Marshal(bp)
	if err != nil {
		t.Fatalf("marshal blueprint: %v", err)
	}
	return string(raw)
}

func TestGadgetWrapperBlueprintSubmitsChildOutcome(t *testing.T) {
	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("gadget", executors.NewGadgetExecutor(engine, gadgetValidationAdapter))
	engine.RegisterExecutor("cel_eval", executors.NewCelEvalExecutor())
	engine.RegisterExecutor("return", executors.NewReturnExecutor())

	parent := dag.Blueprint{
		ID:      "gadget-wrapper-submit",
		Version: 1,
		Nodes: []dag.NodeDef{
			{
				ID:   "run_child",
				Type: "gadget",
				Config: map[string]any{
					"blueprint_json_key": "inputs.child_blueprint_json",
				},
			},
			{
				ID:   "return_result",
				Type: "return",
				Config: map[string]any{
					"from_key": "results.run_child.return_json",
				},
			},
			{
				ID:   "defer_runtime",
				Type: "return",
				Config: map[string]any{
					"value": map[string]any{
						"status": "deferred",
						"reason": "child run failed",
					},
				},
			},
		},
		Edges: []dag.EdgeDef{
			{From: "run_child", To: "return_result", Condition: "results.run_child.status == 'success' && results.run_child.run_status == 'completed'"},
			{From: "run_child", To: "defer_runtime", Condition: "results.run_child.status != 'success' || results.run_child.run_status != 'completed'"},
		},
	}
	parentRaw, err := json.Marshal(parent)
	if err != nil {
		t.Fatal(err)
	}
	if validation := ValidateResolutionBlueprint(parent, parentRaw); !validation.Valid {
		t.Fatalf("expected valid parent blueprint, got issues: %+v", validation.Issues)
	}

	child := dag.Blueprint{
		ID:      "child-submit",
		Version: 1,
		Budget:  &dag.Budget{MaxTotalTimeSeconds: 5, MaxTotalTokens: 10},
		Nodes: []dag.NodeDef{
			{
				ID:   "pick",
				Type: "cel_eval",
				Config: map[string]any{
					"expressions": map[string]string{
						"outcome": "'1'",
					},
				},
			},
			{
				ID:   "done",
				Type: "return",
				Config: map[string]any{
					"value": map[string]any{
						"status":  "success",
						"outcome": "{{results.pick.outcome}}",
					},
				},
			},
		},
		Edges: []dag.EdgeDef{{From: "pick", To: "done"}},
	}

	run, err := engine.Execute(context.Background(), parent, map[string]string{
		"child_blueprint_json": mustBlueprintJSONMain(t, child),
	})
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed parent run, got %+v", run)
	}
	if run.NodeStates["run_child"].Status != "completed" {
		t.Fatalf("expected gadget node completed, got %+v", run.NodeStates["run_child"])
	}
	if run.NodeStates["return_result"].Status != "completed" {
		t.Fatalf("expected return_result node completed, got %+v", run.NodeStates["return_result"])
	}
	if testResultValue(run, "run_child", "return_json") == "" {
		t.Fatalf("expected propagated child return_json, got context %+v", run.Results)
	}
	if returnStringField(t, run.Return, "outcome") != "1" {
		t.Fatalf("expected parent return outcome 1, got %+v", run)
	}
}

func TestGadgetWrapperBlueprintCancelsOnChildFailure(t *testing.T) {
	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("gadget", executors.NewGadgetExecutor(engine, gadgetValidationAdapter))
	engine.RegisterExecutor("return", executors.NewReturnExecutor())

	parent := dag.Blueprint{
		ID:      "gadget-wrapper-cancel",
		Version: 1,
		Nodes: []dag.NodeDef{
			{
				ID:   "run_child",
				Type: "gadget",
				Config: map[string]any{
					"blueprint_json_key": "inputs.child_blueprint_json",
				},
			},
			{
				ID:   "return_result",
				Type: "return",
				Config: map[string]any{
					"from_key": "results.run_child.return_json",
				},
			},
			{
				ID:   "defer_runtime",
				Type: "return",
				Config: map[string]any{
					"value": map[string]any{
						"status": "deferred",
						"reason": "child run failed",
					},
				},
			},
		},
		Edges: []dag.EdgeDef{
			{From: "run_child", To: "return_result", Condition: "results.run_child.status == 'success' && results.run_child.run_status == 'completed'"},
			{From: "run_child", To: "defer_runtime", Condition: "results.run_child.status != 'success' || results.run_child.run_status != 'completed'"},
		},
	}

	run, err := engine.Execute(context.Background(), parent, map[string]string{
		"child_blueprint_json": "{not-json",
	})
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed parent run, got %+v", run)
	}
	if run.NodeStates["defer_runtime"].Status != "completed" {
		t.Fatalf("expected defer_runtime path to run, got %+v", run.NodeStates["defer_runtime"])
	}
	if testResultValue(run, "run_child", "status") != "failed" {
		t.Fatalf("expected gadget soft failure, got %+v", run.Results)
	}
	if returnStringField(t, run.Return, "reason") != "child run failed" {
		t.Fatalf("expected deferred return output, got %+v", run)
	}
}
