package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/qmrkt/resolution-engine/dag"
)

type gadgetTestExecutorFunc func(context.Context, dag.NodeDef, *dag.Invocation) (dag.ExecutorResult, error)

func (fn gadgetTestExecutorFunc) Execute(ctx context.Context, node dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	return fn(ctx, node, inv)
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

func mustGadgetReturnPayload(t *testing.T, outputs map[string]string) map[string]any {
	t.Helper()
	raw := strings.TrimSpace(outputs["return_json"])
	if raw == "" {
		t.Fatalf("expected non-empty return_json on gadget outputs: %+v", outputs)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("unmarshal return_json: %v\n%s", err, raw)
	}
	return payload
}

func newGadgetTestEngine() *dag.Engine {
	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("cel_eval", NewCelEvalExecutor())
	engine.RegisterExecutor("return", NewReturnExecutor())
	exec := NewGadgetExecutor(engine, validGadgetTestValidator)
	engine.RegisterExecutor("gadget", exec)
	return engine
}

// nestedGrandchildBlueprint: pick an outcome and return a success payload.
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
				Type: "return",
				Config: map[string]any{
					"value": map[string]any{
						"status":  "success",
						"outcome": "{{results.pick.outcome}}",
					},
				},
			},
		},
		Edges: []dag.EdgeDef{{From: "pick", To: "submit"}},
	}
}

// nestedChildBlueprint: parent of a nested gadget. The inner gadget's
// return value lands on inner.return_json; the parent branches on it.
func nestedChildBlueprint(innerMaxDepth int) dag.Blueprint {
	innerConfig := map[string]any{
		"inline": nestedGrandchildBlueprint(),
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
				Type: "return",
				Config: map[string]any{
					"from_key": "results.inner.return_json",
				},
			},
			{
				ID:   "defer",
				Type: "return",
				Config: map[string]any{
					"value": map[string]any{
						"status": "deferred",
						"reason": "inner gadget failed",
					},
				},
			},
		},
		Edges: []dag.EdgeDef{
			{From: "inner", To: "submit", Condition: "results.inner.run_status == 'completed'"},
			{From: "inner", To: "defer", Condition: "results.inner.run_status != 'completed'"},
		},
	}
}

func TestGadgetRuntimeBlueprintJSONKey(t *testing.T) {
	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("cel_eval", NewCelEvalExecutor())
	engine.RegisterExecutor("return", NewReturnExecutor())
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
						"answer": "inputs.subject",
					},
				},
			},
			{
				ID:   "out",
				Type: "return",
				Config: map[string]any{
					"value": map[string]any{
						"status": "success",
						"answer": "{{results.step.answer}}",
					},
				},
			},
		},
		Edges: []dag.EdgeDef{{From: "step", To: "out"}},
	}

	node := dag.NodeDef{
		ID:   "run_child",
		Type: "gadget",
		Config: map[string]any{
			"blueprint_json_key": "inputs.candidate.blueprint_json",
			"input_mappings": map[string]string{
				"subject": "inputs.parent.subject",
			},
		},
	}
	inv := dag.NewInvocationFromInputs(map[string]string{
		"candidate.blueprint_json": mustBlueprintJSON(t, child),
		"parent.subject":           "winner",
	})

	result, err := exec.Execute(context.Background(), node, inv)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("expected success, got %+v", result.Outputs)
	}
	if result.Outputs["run_status"] != "completed" {
		t.Fatalf("expected completed child run, got %+v", result.Outputs)
	}
	payload := mustGadgetReturnPayload(t, result.Outputs)
	if payload["answer"] != "winner" {
		t.Fatalf("expected return.answer=winner, got %+v", payload)
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
			"blueprint_json_key": "inputs.candidate.blueprint_json",
		},
	}
	inv := dag.NewInvocationFromInputs(map[string]string{
		"candidate.blueprint_json": "{not-json",
	})

	result, err := exec.Execute(context.Background(), node, inv)
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
			"blueprint_json_key": "inputs.candidate.blueprint_json",
		},
	}
	inv := dag.NewInvocationFromInputs(map[string]string{
		"candidate.blueprint_json": `{"id":"child","version":1,"nodes":[],"edges":[]}`,
	})

	result, err := exec.Execute(context.Background(), node, inv)
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
	inv := dag.NewInvocationFromInputs(map[string]string{
		gadgetDepthKey: "1",
	})

	result, err := exec.Execute(context.Background(), node, inv)
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
					"blueprint_json_key": "inputs.child",
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

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
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
			"blueprint_json": mustBlueprintJSON(t, nestedChildBlueprint(2)),
			"max_depth":      2,
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
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
			"blueprint_json": mustBlueprintJSON(t, nestedChildBlueprint(2)),
			"max_depth":      2,
			"dynamic_blueprint_policy": map[string]any{
				"allowed_node_types":     []string{"gadget", "return"},
				"max_nodes":              8,
				"max_edges":              8,
				"max_total_time_seconds": 10,
				"max_total_tokens":       20,
				"max_depth":              2,
				"allow_agent_loop":       false,
			},
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("expected success, got %+v", result.Outputs)
	}
	payload := mustGadgetReturnPayload(t, result.Outputs)
	if payload["status"] != "success" || payload["outcome"] != "1" {
		t.Fatalf("expected nested success payload, got %+v", payload)
	}
}

func TestGadgetNestedGadgetNeedsMaxDepthBump(t *testing.T) {
	engine := newGadgetTestEngine()
	exec := NewGadgetExecutor(engine, validGadgetTestValidator)
	node := dag.NodeDef{
		ID:   "run_child",
		Type: "gadget",
		Config: map[string]any{
			"blueprint_json": mustBlueprintJSON(t, nestedChildBlueprint(0)),
			"max_depth":      2,
			"dynamic_blueprint_policy": map[string]any{
				"allowed_node_types":     []string{"gadget", "return"},
				"max_nodes":              8,
				"max_edges":              8,
				"max_total_time_seconds": 10,
				"max_total_tokens":       20,
				"max_depth":              2,
				"allow_agent_loop":       false,
			},
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("expected outer gadget success, got %+v", result.Outputs)
	}
	payload := mustGadgetReturnPayload(t, result.Outputs)
	if payload["status"] != "deferred" || payload["reason"] != "inner gadget failed" {
		t.Fatalf("expected defer fallback payload, got %+v", payload)
	}
}

func TestGadgetSurfacesChildReturn(t *testing.T) {
	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("cel_eval", NewCelEvalExecutor())
	engine.RegisterExecutor("return", NewReturnExecutor())
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
				ID:   "out",
				Type: "return",
				Config: map[string]any{
					"value": map[string]any{
						"status":  "success",
						"outcome": "{{results.pick.outcome}}",
					},
				},
			},
		},
		Edges: []dag.EdgeDef{{From: "pick", To: "out"}},
	}
	node := dag.NodeDef{
		ID:   "run_child",
		Type: "gadget",
		Config: map[string]any{
			"blueprint_json": mustBlueprintJSON(t, child),
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("expected success, got %+v", result.Outputs)
	}
	payload := mustGadgetReturnPayload(t, result.Outputs)
	if payload["status"] != "success" || payload["outcome"] != "1" {
		t.Fatalf("expected propagated payload, got %+v", payload)
	}
}

func TestGadgetAllowsInlineWaitByDefault(t *testing.T) {
	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("wait", NewWaitExecutor())
	engine.RegisterExecutor("return", NewReturnExecutor())
	exec := NewGadgetExecutor(engine, validGadgetTestValidator)

	child := dag.Blueprint{
		ID:      "child-inline-wait",
		Version: 1,
		Budget:  &dag.Budget{MaxTotalTimeSeconds: 5, MaxTotalTokens: 10},
		Nodes: []dag.NodeDef{
			{
				ID:     "pause",
				Type:   "wait",
				Config: map[string]any{"duration_seconds": 0},
			},
			{
				ID:   "out",
				Type: "return",
				Config: map[string]any{
					"value": map[string]any{
						"status": "success",
						"waited": "{{results.pause.waited}}",
						"mode":   "{{results.pause.mode}}",
					},
				},
			},
		},
		Edges: []dag.EdgeDef{{From: "pause", To: "out"}},
	}
	node := dag.NodeDef{
		ID:   "run_child",
		Type: "gadget",
		Config: map[string]any{
			"blueprint_json": mustBlueprintJSON(t, child),
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("expected success, got %+v", result.Outputs)
	}
	payload := mustGadgetReturnPayload(t, result.Outputs)
	if payload["waited"] != "0" {
		t.Fatalf("payload.waited = %v, want \"0\"", payload["waited"])
	}
	if payload["mode"] != "sleep" {
		t.Fatalf("payload.mode = %v, want \"sleep\"", payload["mode"])
	}
}

func TestGadgetSoftFailsChildExecutionError(t *testing.T) {
	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("fail_step", gadgetTestExecutorFunc(func(_ context.Context, _ dag.NodeDef, _ *dag.Invocation) (dag.ExecutorResult, error) {
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
				"allow_agent_loop":       false,
			},
		},
	}

	result, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
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
	engine.RegisterExecutor("inspect", gadgetTestExecutorFunc(func(_ context.Context, _ dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
		return dag.ExecutorResult{Outputs: map[string]string{
			"status":  "success",
			"depth":   inv.Run.Inputs[gadgetDepthKey],
			"subject": inv.Run.Inputs["subject"],
		}}, nil
	}))
	engine.RegisterExecutor("return", NewReturnExecutor())
	exec := NewGadgetExecutor(engine, validGadgetTestValidator)

	child := dag.Blueprint{
		ID:      "child-inspect",
		Version: 1,
		Budget:  &dag.Budget{MaxTotalTimeSeconds: 5, MaxTotalTokens: 10},
		Nodes: []dag.NodeDef{
			{ID: "inspect", Type: "inspect", Config: map[string]any{}},
			{
				ID:   "out",
				Type: "return",
				Config: map[string]any{
					"value": map[string]any{
						"status":  "success",
						"depth":   "{{results.inspect.depth}}",
						"subject": "{{results.inspect.subject}}",
					},
				},
			},
		},
		Edges: []dag.EdgeDef{{From: "inspect", To: "out"}},
	}
	node := dag.NodeDef{
		ID:   "run_child",
		Type: "gadget",
		Config: map[string]any{
			"blueprint_json": mustBlueprintJSON(t, child),
			"input_mappings": map[string]string{
				gadgetDepthKey: "inputs.parent.fake_depth",
				"subject":      "inputs.parent.subject",
			},
			"dynamic_blueprint_policy": map[string]any{
				"allowed_node_types":     []string{"inspect", "return"},
				"max_nodes":              4,
				"max_edges":              4,
				"max_total_time_seconds": 5,
				"max_total_tokens":       10,
				"allow_agent_loop":       false,
			},
		},
	}
	inv := dag.NewInvocationFromInputs(map[string]string{
		"parent.fake_depth": "999",
		"parent.subject":    "winner",
	})

	result, err := exec.Execute(context.Background(), node, inv)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("expected success, got %+v", result.Outputs)
	}
	payload := mustGadgetReturnPayload(t, result.Outputs)
	if payload["depth"] != "1" {
		t.Fatalf("expected reserved depth override to win, got %+v", payload)
	}
	if payload["subject"] != "winner" {
		t.Fatalf("expected mapped input to pass through, got %+v", payload)
	}
}

func TestGadgetRejectsPolicyDepthExceeded(t *testing.T) {
	engine := dag.NewEngine(nil)
	engine.RegisterExecutor("cel_eval", NewCelEvalExecutor())
	engine.RegisterExecutor("return", NewReturnExecutor())
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
			{
				ID:   "out",
				Type: "return",
				Config: map[string]any{
					"value": map[string]any{
						"status": "success",
						"answer": "{{results.step.answer}}",
					},
				},
			},
		},
		Edges: []dag.EdgeDef{{From: "step", To: "out"}},
	}
	node := dag.NodeDef{
		ID:   "run_child",
		Type: "gadget",
		Config: map[string]any{
			"blueprint_json": mustBlueprintJSON(t, child),
			"max_depth":      2,
			"dynamic_blueprint_policy": map[string]any{
				"allowed_node_types":     []string{"cel_eval", "return"},
				"max_nodes":              4,
				"max_edges":              4,
				"max_total_time_seconds": 5,
				"max_total_tokens":       10,
				"max_depth":              1,
				"allow_agent_loop":       false,
			},
		},
	}
	inv := dag.NewInvocationFromInputs(map[string]string{
		gadgetDepthKey: "1",
	})

	result, err := exec.Execute(context.Background(), node, inv)
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
