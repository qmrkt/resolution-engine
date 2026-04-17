package executors

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/qmrkt/resolution-engine/dag"
)

func TestReturnExecutorValueShape(t *testing.T) {
	exec := NewReturnExecutor()
	ctx := dag.NewInvocation(dag.Run{})
	ctx.Results.Record("agent", map[string]string{"outcome": "1"})
	node := dag.NodeDef{
		ID:   "submit",
		Type: "return",
		Config: map[string]any{
			"value": map[string]any{
				"status":  "success",
				"outcome": "{{results.agent.outcome}}",
			},
		},
	}
	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.EarlyReturn == nil {
		t.Fatal("expected EarlyReturn payload")
	}
	if result.Suspend != nil {
		t.Fatal("return must not Suspend")
	}
	var payload map[string]any
	if err := json.Unmarshal(*result.EarlyReturn, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["status"] != "success" || payload["outcome"] != "1" {
		t.Fatalf("payload = %+v, want {status:success, outcome:1}", payload)
	}
}

func TestReturnExecutorFromKey(t *testing.T) {
	exec := NewReturnExecutor()
	ctx := dag.NewInvocation(dag.Run{})
	ctx.Results.Record("agent", map[string]string{
		"output_json": `{"status":"deferred","reason":"need more time"}`,
	})
	node := dag.NodeDef{
		ID:   "passthrough",
		Type: "return",
		Config: map[string]any{
			"from_key": "results.agent.output_json",
		},
	}
	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.EarlyReturn == nil {
		t.Fatal("expected EarlyReturn payload")
	}
	var payload map[string]any
	if err := json.Unmarshal(*result.EarlyReturn, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["status"] != "deferred" || payload["reason"] != "need more time" {
		t.Fatalf("payload = %+v, want passthrough", payload)
	}
}

func TestReturnExecutorRecursiveInterpolation(t *testing.T) {
	exec := NewReturnExecutor()
	ctx := dag.NewInvocation(dag.Run{})
	ctx.Results.Record("a", map[string]string{
		"outcome":  "0",
		"evidence": "primary source",
	})
	ctx.Results.Record("b", map[string]string{"label": "alpha"})
	node := dag.NodeDef{
		ID:   "out",
		Type: "return",
		Config: map[string]any{
			"value": map[string]any{
				"status": "success",
				"nested": map[string]any{
					"outcome":  "{{results.a.outcome}}",
					"evidence": "{{results.a.evidence}}",
				},
				"labels": []any{"{{results.b.label}}", "static"},
			},
		},
	}
	result, err := exec.Execute(context.Background(), node, ctx)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(*result.EarlyReturn, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	nested, ok := payload["nested"].(map[string]any)
	if !ok {
		t.Fatalf("nested not a map: %+v", payload)
	}
	if nested["outcome"] != "0" || nested["evidence"] != "primary source" {
		t.Fatalf("nested interpolation failed: %+v", nested)
	}
	labels, ok := payload["labels"].([]any)
	if !ok || len(labels) != 2 {
		t.Fatalf("labels not a 2-element list: %+v", payload["labels"])
	}
	if labels[0] != "alpha" || labels[1] != "static" {
		t.Fatalf("labels interpolation failed: %+v", labels)
	}
}

func TestReturnExecutorRejectsBothValueAndFromKey(t *testing.T) {
	exec := NewReturnExecutor()
	node := dag.NodeDef{
		ID:   "bad",
		Type: "return",
		Config: map[string]any{
			"value":    map[string]any{"status": "success"},
			"from_key": "anywhere",
		},
	}
	_, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err == nil {
		t.Fatal("expected error when both value and from_key are set")
	}
	if !strings.Contains(err.Error(), "exactly one of value or from_key") {
		t.Fatalf("error = %v, want xor message", err)
	}
}

func TestReturnExecutorRejectsNeither(t *testing.T) {
	exec := NewReturnExecutor()
	node := dag.NodeDef{
		ID:     "bad",
		Type:   "return",
		Config: map[string]any{},
	}
	_, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err == nil {
		t.Fatal("expected error when neither value nor from_key are set")
	}
}

func TestReturnExecutorRejectsMissingStatus(t *testing.T) {
	exec := NewReturnExecutor()
	node := dag.NodeDef{
		ID:   "bad",
		Type: "return",
		Config: map[string]any{
			"value": map[string]any{
				"outcome": "1",
			},
		},
	}
	_, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err == nil {
		t.Fatal("expected error when value.status is missing")
	}
}

func TestReturnExecutorRejectsEmptyStatus(t *testing.T) {
	exec := NewReturnExecutor()
	node := dag.NodeDef{
		ID:   "bad",
		Type: "return",
		Config: map[string]any{
			"value": map[string]any{
				"status": "   ",
			},
		},
	}
	_, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err == nil {
		t.Fatal("expected error when value.status is whitespace-only")
	}
}

func TestReturnExecutorRejectsNonStringStatus(t *testing.T) {
	exec := NewReturnExecutor()
	node := dag.NodeDef{
		ID:   "bad",
		Type: "return",
		Config: map[string]any{
			"value": map[string]any{
				"status": 42,
			},
		},
	}
	_, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err == nil {
		t.Fatal("expected error when value.status is not a string")
	}
}

func TestReturnExecutorRejectsNonObjectValue(t *testing.T) {
	exec := NewReturnExecutor()
	node := dag.NodeDef{
		ID:   "bad",
		Type: "return",
		Config: map[string]any{
			"value": "scalar",
		},
	}
	_, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err == nil {
		t.Fatal("expected error when value is not a JSON object")
	}
}

func TestReturnExecutorFromKeyEmpty(t *testing.T) {
	exec := NewReturnExecutor()
	node := dag.NodeDef{
		ID:   "bad",
		Type: "return",
		Config: map[string]any{
			"from_key": "inputs.missing",
		},
	}
	_, err := exec.Execute(context.Background(), node, dag.NewInvocationFromInputs(nil))
	if err == nil {
		t.Fatal("expected error when from_key resolves to empty")
	}
}

func TestReturnExecutorFromKeyInvalidJSON(t *testing.T) {
	exec := NewReturnExecutor()
	ctx := dag.NewInvocationFromInputs(map[string]string{"raw.bad": "{not json"})
	node := dag.NodeDef{
		ID:   "bad",
		Type: "return",
		Config: map[string]any{
			"from_key": "inputs.raw.bad",
		},
	}
	_, err := exec.Execute(context.Background(), node, ctx)
	if err == nil {
		t.Fatal("expected error when from_key payload is invalid JSON")
	}
}

func TestReturnExecutorFromKeyNonObject(t *testing.T) {
	exec := NewReturnExecutor()
	ctx := dag.NewInvocationFromInputs(map[string]string{"raw.scalar": `"just a string"`})
	node := dag.NodeDef{
		ID:   "bad",
		Type: "return",
		Config: map[string]any{
			"from_key": "inputs.raw.scalar",
		},
	}
	_, err := exec.Execute(context.Background(), node, ctx)
	if err == nil {
		t.Fatal("expected error when from_key payload is not an object")
	}
}
