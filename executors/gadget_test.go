package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/qmrkt/resolution-engine/dag"
)

type gadgetTestExecutorFunc func(context.Context, dag.NodeDef, *dag.Context) (dag.ExecutorResult, error)

func (fn gadgetTestExecutorFunc) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	return fn(ctx, node, execCtx)
}

func validGadgetTestValidator(_ dag.Blueprint, _ []byte) BlueprintValidationResult {
	return BlueprintValidationResult{Valid: true, Issues: []BlueprintValidationIssue{}}
}

func mustBlueprintJSON(t *testing.T, bp dag.Blueprint) string {
	t.Helper()
	raw, err := json.Marshal(bp)
	if err != nil {
		t.Fatalf("marshal blueprint: %v", err)
	}
	return string(raw)
}

func newGadgetTestEngine() *dag.Engine {
	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("cel_eval", NewCelEvalExecutor())
	engine.RegisterExecutor("submit_result", NewSubmitResultExecutor())
	engine.RegisterExecutor("defer_resolution", NewDeferResolutionExecutor())
	engine.RegisterExecutor("cancel_market", NewCancelMarketExecutor())
	exec := NewGadgetExecutor(engine, validGadgetTestValidator)
	engine.RegisterExecutor("gadget", exec)
	return engine
}

func nestedGrandchildBlueprint() dag.Blueprint {
	return dag.Blueprint{
		ID:      "grandchild",
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
}

func nestedChildBlueprint(innerMaxDepth int) dag.Blueprint {
	innerConfig := map[string]any{
		"inline":             nestedGrandchildBlueprint(),
		"propagate_terminal": true,
	}
	if innerMaxDepth > 0 {
		innerConfig["max_depth"] = innerMaxDepth
	}

	return dag.Blueprint{
		ID:      "child-gadget",
		Version: 1,
		Budget:  &dag.Budget{MaxTotalTimeSeconds: 5, MaxTotalTokens: 10},
		Nodes: []dag.NodeDef{
			{
				ID:     "inner",
				Type:   "gadget",
				Config: innerConfig,
			},
			{
				ID:   "submit",
				Type: "submit_result",
				Config: map[string]any{
					"outcome_key": "inner.outcome",
				},
			},
			{
				ID:   "defer",
				Type: "defer_resolution",
				Config: map[string]any{
					"reason": "inner gadget failed",
				},
			},
		},
		Edges: []dag.EdgeDef{
			{From: "inner", To: "submit", Condition: "inner.submitted == 'true'"},
			{From: "inner", To: "defer", Condition: "inner.submitted != 'true'"},
		},
	}
}

func TestGadgetRuntimeBlueprintJSONKey(t *testing.T) {
	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("cel_eval", NewCelEvalExecutor())
	exec := NewGadgetExecutor(engine, validGadgetTestValidator)

	child := dag.Blueprint{
		ID:      "child",
		Version: 1,
		Budget:  &dag.Budget{MaxTotalTimeSeconds: 5, MaxTotalTokens: 10},
		Nodes: []dag.NodeDef{
			{
				ID:   "step",
				Type: "cel_eval",
				Config: map[string]any{
					"expressions": map[string]string{
						"answer": "input.subject",
					},
				},
			},
		},
	}

	node := dag.NodeDef{
		ID:   "run_child",
		Type: "gadget",
		Config: map[string]any{
			"blueprint_json_key": "candidate.blueprint_json",
			"input_mappings": map[string]string{
				"subject": "parent.subject",
			},
			"output_keys": []string{"step.answer"},
		},
	}
	execCtx := dag.NewContext(map[string]string{
		"candidate.blueprint_json": mustBlueprintJSON(t, child),
		"parent.subject":           "winner",
	})

	result, err := exec.Execute(context.Background(), node, execCtx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("expected success, got %+v", result.Outputs)
	}
	if result.Outputs["run_status"] != "completed" {
		t.Fatalf("expected completed child run, got %+v", result.Outputs)
	}
	if result.Outputs["step.answer"] != "winner" {
		t.Fatalf("expected mapped child output, got %+v", result.Outputs)
	}
	if strings.TrimSpace(result.Outputs["child_run_id"]) == "" {
		t.Fatalf("expected child_run_id, got %+v", result.Outputs)
	}
}

func TestGadgetInvalidRuntimeJSON(t *testing.T) {
	exec := NewGadgetExecutor(dag.NewEngine(nil), validGadgetTestValidator)
	node := dag.NodeDef{
		ID:   "run_child",
		Type: "gadget",
		Config: map[string]any{
			"blueprint_json_key": "candidate.blueprint_json",
		},
	}
	execCtx := dag.NewContext(map[string]string{
		"candidate.blueprint_json": "{not-json",
	})

	result, err := exec.Execute(context.Background(), node, execCtx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "failed" {
		t.Fatalf("expected failed status, got %+v", result.Outputs)
	}
	if !strings.Contains(result.Outputs["error"], "valid JSON") {
		t.Fatalf("expected JSON parse failure, got %+v", result.Outputs)
	}
}

func TestGadgetValidatorRejectsInvalidChild(t *testing.T) {
	exec := NewGadgetExecutor(dag.NewEngine(nil), func(_ dag.Blueprint, _ []byte) BlueprintValidationResult {
		return BlueprintValidationResult{
			Valid: false,
			Issues: []BlueprintValidationIssue{
				{Code: "BAD_CHILD", Message: "child blueprint is not acceptable", Target: "child"},
			},
		}
	})
	node := dag.NodeDef{
		ID:   "run_child",
		Type: "gadget",
		Config: map[string]any{
			"blueprint_json_key": "candidate.blueprint_json",
		},
	}
	execCtx := dag.NewContext(map[string]string{
		"candidate.blueprint_json": `{"id":"child","version":1,"nodes":[],"edges":[]}`,
	})

	result, err := exec.Execute(context.Background(), node, execCtx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "failed" {
		t.Fatalf("expected failed status, got %+v", result.Outputs)
	}
	if result.Outputs["valid"] != "false" {
		t.Fatalf("expected valid=false, got %+v", result.Outputs)
	}
	if result.Outputs["first_issue_code"] != "BAD_CHILD" {
		t.Fatalf("expected first issue code, got %+v", result.Outputs)
	}
}

func TestGadgetDepthGuard(t *testing.T) {
	exec := NewGadgetExecutor(dag.NewEngine(nil), validGadgetTestValidator)
	node := dag.NodeDef{
		ID:   "run_child",
		Type: "gadget",
		Config: map[string]any{
			"blueprint_json": `{"id":"child","version":1,"nodes":[],"edges":[]}`,
			"max_depth":      1,
		},
	}
	execCtx := dag.NewContext(map[string]string{
		gadgetDepthKey: "1",
	})

	result, err := exec.Execute(context.Background(), node, execCtx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "failed" {
		t.Fatalf("expected failed status, got %+v", result.Outputs)
	}
	if !strings.Contains(result.Outputs["error"], "max_depth 1") {
		t.Fatalf("expected max depth failure, got %+v", result.Outputs)
	}
}

func TestGadgetRejectsDisallowedNodeType(t *testing.T) {
	exec := NewGadgetExecutor(dag.NewEngine(nil), validGadgetTestValidator)
	child := dag.Blueprint{
		ID:      "child",
		Version: 1,
		Budget:  &dag.Budget{MaxTotalTimeSeconds: 5, MaxTotalTokens: 10},
		Nodes: []dag.NodeDef{
			{
				ID:   "check",
				Type: "validate_blueprint",
				Config: map[string]any{
					"blueprint_json_key": "input.child",
				},
			},
		},
	}
	node := dag.NodeDef{
		ID:   "run_child",
		Type: "gadget",
		Config: map[string]any{
			"blueprint_json": mustBlueprintJSON(t, child),
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "failed" {
		t.Fatalf("expected failed status, got %+v", result.Outputs)
	}
	if !strings.Contains(result.Outputs["error"], `node type "validate_blueprint" is not allowed`) {
		t.Fatalf("expected policy rejection, got %+v", result.Outputs)
	}
}

func TestGadgetRejectsNestedGadgetByDefaultPolicy(t *testing.T) {
	engine := newGadgetTestEngine()
	exec := NewGadgetExecutor(engine, validGadgetTestValidator)
	node := dag.NodeDef{
		ID:   "run_child",
		Type: "gadget",
		Config: map[string]any{
			"blueprint_json":     mustBlueprintJSON(t, nestedChildBlueprint(2)),
			"propagate_terminal": true,
			"max_depth":          2,
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "failed" {
		t.Fatalf("expected failed status, got %+v", result.Outputs)
	}
	if !strings.Contains(result.Outputs["error"], `node type "gadget" is not allowed`) {
		t.Fatalf("expected nested gadget policy rejection, got %+v", result.Outputs)
	}
}

func TestGadgetSupportsNestedGadgetWithCustomPolicy(t *testing.T) {
	engine := newGadgetTestEngine()
	exec := NewGadgetExecutor(engine, validGadgetTestValidator)
	node := dag.NodeDef{
		ID:   "run_child",
		Type: "gadget",
		Config: map[string]any{
			"blueprint_json":     mustBlueprintJSON(t, nestedChildBlueprint(2)),
			"propagate_terminal": true,
			"max_depth":          2,
			"dynamic_blueprint_policy": map[string]any{
				"allowed_node_types":     []string{"gadget", "submit_result", "defer_resolution"},
				"max_nodes":              8,
				"max_edges":              8,
				"max_total_time_seconds": 10,
				"max_total_tokens":       20,
				"max_depth":              2,
				"allow_terminal_nodes":   true,
				"allow_agent_loop":       false,
			},
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("expected success, got %+v", result.Outputs)
	}
	if result.Outputs["terminal_action"] != "submit_result" || result.Outputs["submitted"] != "true" {
		t.Fatalf("expected propagated submit result, got %+v", result.Outputs)
	}
	if result.Outputs["outcome"] != "1" {
		t.Fatalf("expected nested gadget outcome 1, got %+v", result.Outputs)
	}
}

func TestGadgetNestedGadgetNeedsMaxDepthBump(t *testing.T) {
	engine := newGadgetTestEngine()
	exec := NewGadgetExecutor(engine, validGadgetTestValidator)
	node := dag.NodeDef{
		ID:   "run_child",
		Type: "gadget",
		Config: map[string]any{
			"blueprint_json":     mustBlueprintJSON(t, nestedChildBlueprint(0)),
			"propagate_terminal": true,
			"max_depth":          2,
			"dynamic_blueprint_policy": map[string]any{
				"allowed_node_types":     []string{"gadget", "submit_result", "defer_resolution"},
				"max_nodes":              8,
				"max_edges":              8,
				"max_total_time_seconds": 10,
				"max_total_tokens":       20,
				"max_depth":              2,
				"allow_terminal_nodes":   true,
				"allow_agent_loop":       false,
			},
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("expected outer gadget success, got %+v", result.Outputs)
	}
	if result.Outputs["terminal_action"] != "defer_resolution" || result.Outputs["deferred"] != "true" {
		t.Fatalf("expected defer fallback, got %+v", result.Outputs)
	}
	if result.Outputs["reason"] != "inner gadget failed" {
		t.Fatalf("expected defer reason, got %+v", result.Outputs)
	}
}

func TestGadgetPropagatesSubmitTerminal(t *testing.T) {
	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("cel_eval", NewCelEvalExecutor())
	engine.RegisterExecutor("submit_result", NewSubmitResultExecutor())
	exec := NewGadgetExecutor(engine, validGadgetTestValidator)

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
	node := dag.NodeDef{
		ID:   "run_child",
		Type: "gadget",
		Config: map[string]any{
			"blueprint_json":     mustBlueprintJSON(t, child),
			"propagate_terminal": true,
			"output_keys":        []string{"pick.outcome", "submit.outcome"},
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("expected success, got %+v", result.Outputs)
	}
	if result.Outputs["terminal_action"] != "submit_result" || result.Outputs["submitted"] != "true" {
		t.Fatalf("expected propagated submit result, got %+v", result.Outputs)
	}
	if result.Outputs["outcome"] != "1" {
		t.Fatalf("expected outcome 1, got %+v", result.Outputs)
	}
	if strings.TrimSpace(result.Outputs["evidence_hash"]) == "" {
		t.Fatalf("expected evidence_hash, got %+v", result.Outputs)
	}
}

func TestGadgetPropagatesCancelAndDeferTerminal(t *testing.T) {
	testCases := []struct {
		name           string
		nodeType       string
		expectedAction string
		expectedFlag   string
		expectedReason string
	}{
		{
			name:           "cancel",
			nodeType:       "cancel_market",
			expectedAction: "cancel_market",
			expectedFlag:   "cancelled",
			expectedReason: "market invalid",
		},
		{
			name:           "defer",
			nodeType:       "defer_resolution",
			expectedAction: "defer_resolution",
			expectedFlag:   "deferred",
			expectedReason: "need more data",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			engine := dag.NewEngine(nil)
			engine.RegisterExecutor("cancel_market", NewCancelMarketExecutor())
			engine.RegisterExecutor("defer_resolution", NewDeferResolutionExecutor())
			exec := NewGadgetExecutor(engine, validGadgetTestValidator)

			child := dag.Blueprint{
				ID:      "child-action",
				Version: 1,
				Budget:  &dag.Budget{MaxTotalTimeSeconds: 5, MaxTotalTokens: 10},
				Nodes: []dag.NodeDef{
					{
						ID:   "act",
						Type: tc.nodeType,
						Config: map[string]any{
							"reason": tc.expectedReason,
						},
					},
				},
			}
			node := dag.NodeDef{
				ID:   "run_child",
				Type: "gadget",
				Config: map[string]any{
					"blueprint_json":     mustBlueprintJSON(t, child),
					"propagate_terminal": true,
				},
			}

			result, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
			if err != nil {
				t.Fatal(err)
			}
			if result.Outputs["status"] != "success" {
				t.Fatalf("expected success, got %+v", result.Outputs)
			}
			if result.Outputs["terminal_action"] != tc.expectedAction {
				t.Fatalf("expected action %q, got %+v", tc.expectedAction, result.Outputs)
			}
			if result.Outputs[tc.expectedFlag] != "true" {
				t.Fatalf("expected flag %q, got %+v", tc.expectedFlag, result.Outputs)
			}
			if result.Outputs["reason"] != tc.expectedReason {
				t.Fatalf("expected reason %q, got %+v", tc.expectedReason, result.Outputs)
			}
		})
	}
}

func TestGadgetSoftFailsChildExecutionError(t *testing.T) {
	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("fail_step", gadgetTestExecutorFunc(func(_ context.Context, _ dag.NodeDef, _ *dag.Context) (dag.ExecutorResult, error) {
		return dag.ExecutorResult{}, fmt.Errorf("boom")
	}))
	exec := NewGadgetExecutor(engine, validGadgetTestValidator)

	child := dag.Blueprint{
		ID:      "child-fail",
		Version: 1,
		Budget:  &dag.Budget{MaxTotalTimeSeconds: 5, MaxTotalTokens: 10},
		Nodes: []dag.NodeDef{
			{ID: "step", Type: "fail_step", Config: map[string]any{}},
		},
	}
	node := dag.NodeDef{
		ID:   "run_child",
		Type: "gadget",
		Config: map[string]any{
			"blueprint_json": mustBlueprintJSON(t, child),
			"dynamic_blueprint_policy": map[string]any{
				"allowed_node_types":     []string{"fail_step"},
				"max_nodes":              4,
				"max_edges":              4,
				"max_total_time_seconds": 5,
				"max_total_tokens":       10,
				"allow_terminal_nodes":   false,
				"allow_agent_loop":       false,
			},
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewContext(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "failed" {
		t.Fatalf("expected failed status, got %+v", result.Outputs)
	}
	if result.Outputs["run_status"] != "failed" {
		t.Fatalf("expected failed child run, got %+v", result.Outputs)
	}
	if !strings.Contains(result.Outputs["error"], "boom") {
		t.Fatalf("expected child execution error, got %+v", result.Outputs)
	}
}

func TestGadgetInputMappingsCannotOverrideReservedDepth(t *testing.T) {
	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("inspect", gadgetTestExecutorFunc(func(_ context.Context, _ dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
		return dag.ExecutorResult{Outputs: map[string]string{
			"status":  "success",
			"depth":   execCtx.Get(gadgetDepthKey),
			"subject": execCtx.Get("subject"),
		}}, nil
	}))
	exec := NewGadgetExecutor(engine, validGadgetTestValidator)

	child := dag.Blueprint{
		ID:      "child-inspect",
		Version: 1,
		Budget:  &dag.Budget{MaxTotalTimeSeconds: 5, MaxTotalTokens: 10},
		Nodes: []dag.NodeDef{
			{ID: "inspect", Type: "inspect", Config: map[string]any{}},
		},
	}
	node := dag.NodeDef{
		ID:   "run_child",
		Type: "gadget",
		Config: map[string]any{
			"blueprint_json": mustBlueprintJSON(t, child),
			"input_mappings": map[string]string{
				gadgetDepthKey: "parent.fake_depth",
				"subject":      "parent.subject",
			},
			"output_keys": []string{"inspect.depth", "inspect.subject"},
			"dynamic_blueprint_policy": map[string]any{
				"allowed_node_types":     []string{"inspect"},
				"max_nodes":              4,
				"max_edges":              4,
				"max_total_time_seconds": 5,
				"max_total_tokens":       10,
				"allow_terminal_nodes":   false,
				"allow_agent_loop":       false,
			},
		},
	}
	execCtx := dag.NewContext(map[string]string{
		"parent.fake_depth": "999",
		"parent.subject":    "winner",
	})

	result, err := exec.Execute(context.Background(), node, execCtx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("expected success, got %+v", result.Outputs)
	}
	if result.Outputs["inspect.depth"] != "1" {
		t.Fatalf("expected reserved depth override to win, got %+v", result.Outputs)
	}
	if result.Outputs["inspect.subject"] != "winner" {
		t.Fatalf("expected mapped input to pass through, got %+v", result.Outputs)
	}
}

func TestGadgetRejectsPolicyDepthExceeded(t *testing.T) {
	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("cel_eval", NewCelEvalExecutor())
	exec := NewGadgetExecutor(engine, validGadgetTestValidator)

	child := dag.Blueprint{
		ID:      "child-depth",
		Version: 1,
		Budget:  &dag.Budget{MaxTotalTimeSeconds: 5, MaxTotalTokens: 10},
		Nodes: []dag.NodeDef{
			{
				ID:   "step",
				Type: "cel_eval",
				Config: map[string]any{
					"expressions": map[string]string{
						"answer": "'ok'",
					},
				},
			},
		},
	}
	node := dag.NodeDef{
		ID:   "run_child",
		Type: "gadget",
		Config: map[string]any{
			"blueprint_json": mustBlueprintJSON(t, child),
			"max_depth":      2,
			"dynamic_blueprint_policy": map[string]any{
				"allowed_node_types":     []string{"cel_eval"},
				"max_nodes":              4,
				"max_edges":              4,
				"max_total_time_seconds": 5,
				"max_total_tokens":       10,
				"max_depth":              1,
				"allow_terminal_nodes":   false,
				"allow_agent_loop":       false,
			},
		},
	}
	execCtx := dag.NewContext(map[string]string{
		gadgetDepthKey: "1",
	})

	result, err := exec.Execute(context.Background(), node, execCtx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "failed" {
		t.Fatalf("expected failed status, got %+v", result.Outputs)
	}
	if !strings.Contains(result.Outputs["error"], "max dynamic blueprint depth 1 exceeded") {
		t.Fatalf("expected policy depth rejection, got %+v", result.Outputs)
	}
}
