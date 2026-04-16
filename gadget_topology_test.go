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
	engine.RegisterExecutor("submit_result", executors.NewSubmitResultExecutor())
	engine.RegisterExecutor("cancel_market", executors.NewCancelMarketExecutor())

	parent := dag.Blueprint{
		ID:      "gadget-wrapper-submit",
		Version: 1,
		Nodes: []dag.NodeDef{
			{
				ID:   "run_child",
				Type: "gadget",
				Config: map[string]any{
					"blueprint_json_key": "input.child_blueprint_json",
					"propagate_terminal": true,
				},
			},
			{
				ID:   "submit",
				Type: "submit_result",
				Config: map[string]any{
					"outcome_key": "run_child.outcome",
				},
			},
			{
				ID:   "cancel",
				Type: "cancel_market",
				Config: map[string]any{
					"reason": "child run failed",
				},
			},
		},
		Edges: []dag.EdgeDef{
			{From: "run_child", To: "submit", Condition: "run_child.submitted == 'true'"},
			{From: "run_child", To: "cancel", Condition: "run_child.submitted != 'true'"},
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
				ID:   "submit",
				Type: "submit_result",
				Config: map[string]any{
					"outcome_key": "pick.outcome",
				},
			},
		},
		Edges: []dag.EdgeDef{{From: "pick", To: "submit"}},
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
	if run.NodeStates["submit"].Status != "completed" {
		t.Fatalf("expected submit node completed, got %+v", run.NodeStates["submit"])
	}
	if run.Context["run_child.submitted"] != "true" {
		t.Fatalf("expected propagated child submission, got context %+v", run.Context)
	}
	if run.Context["submit.outcome"] != "1" {
		t.Fatalf("expected parent submit outcome 1, got %+v", run.Context)
	}
}

func TestGadgetWrapperBlueprintCancelsOnChildFailure(t *testing.T) {
	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("gadget", executors.NewGadgetExecutor(engine, gadgetValidationAdapter))
	engine.RegisterExecutor("submit_result", executors.NewSubmitResultExecutor())
	engine.RegisterExecutor("cancel_market", executors.NewCancelMarketExecutor())

	parent := dag.Blueprint{
		ID:      "gadget-wrapper-cancel",
		Version: 1,
		Nodes: []dag.NodeDef{
			{
				ID:   "run_child",
				Type: "gadget",
				Config: map[string]any{
					"blueprint_json_key": "input.child_blueprint_json",
					"propagate_terminal": true,
				},
			},
			{
				ID:   "submit",
				Type: "submit_result",
				Config: map[string]any{
					"outcome_key": "run_child.outcome",
				},
			},
			{
				ID:   "cancel",
				Type: "cancel_market",
				Config: map[string]any{
					"reason": "child run failed",
				},
			},
		},
		Edges: []dag.EdgeDef{
			{From: "run_child", To: "submit", Condition: "run_child.submitted == 'true'"},
			{From: "run_child", To: "cancel", Condition: "run_child.submitted != 'true'"},
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
	if run.NodeStates["cancel"].Status != "completed" {
		t.Fatalf("expected cancel path to run, got %+v", run.NodeStates["cancel"])
	}
	if run.Context["run_child.status"] != "failed" {
		t.Fatalf("expected gadget soft failure, got %+v", run.Context)
	}
	if run.Context["cancel.cancelled"] != "true" {
		t.Fatalf("expected cancel output, got %+v", run.Context)
	}
}
