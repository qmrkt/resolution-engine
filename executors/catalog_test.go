package executors

import (
	"encoding/json"
	"testing"

	"github.com/qmrkt/resolution-engine/dag"
)

// describedExecutorCases returns every production executor paired with the
// node type it registers under. The conformance test asserts each entry
// implements both ConfigSchemaProvider and OutputKeyProvider and produces
// valid, non-empty declarations.
func describedExecutorCases(t *testing.T) []struct {
	nodeType string
	exec     dag.Executor
} {
	t.Helper()
	return []struct {
		nodeType string
		exec     dag.Executor
	}{
		{"api_fetch", NewAPIFetchExecutor()},
		{"llm_call", NewLLMCallExecutorWithConfig(LLMCallExecutorConfig{})},
		{"agent_loop", &AgentLoopExecutor{}},
		{"return", NewReturnExecutor()},
		{"await_signal", NewAwaitSignalExecutor()},
		{"wait", NewWaitExecutor()},
		{"cel_eval", NewCelEvalExecutor()},
		{"map", NewMapExecutor(dag.NewEngine(nil))},
		{"gadget", NewGadgetExecutor(dag.NewEngine(nil), nil)},
		{"validate_blueprint", NewValidateBlueprintExecutor(func(bp dag.Blueprint, raw []byte) BlueprintValidationResult {
			return BlueprintValidationResult{Valid: true}
		})},
	}
}

// TestAllExecutorsProvideConfigSchema asserts every production executor
// implements dag.ConfigSchemaProvider and returns a syntactically valid JSON
// Schema object.
func TestAllExecutorsProvideConfigSchema(t *testing.T) {
	for _, tc := range describedExecutorCases(t) {
		t.Run(tc.nodeType, func(t *testing.T) {
			provider, ok := tc.exec.(dag.ConfigSchemaProvider)
			if !ok {
				t.Fatalf("executor for %q does not implement ConfigSchemaProvider", tc.nodeType)
			}
			schema := provider.ConfigSchema()
			if len(schema) == 0 {
				t.Fatal("ConfigSchema returned empty payload")
			}
			var obj map[string]interface{}
			if err := json.Unmarshal(schema, &obj); err != nil {
				t.Fatalf("ConfigSchema is not valid JSON: %v\n%s", err, string(schema))
			}
			if obj["type"] != "object" {
				t.Fatalf("ConfigSchema top-level type = %v, want \"object\"", obj["type"])
			}
		})
	}
}

// TestAllExecutorsProvideOutputKeys asserts every production executor
// implements dag.OutputKeyProvider and advertises at least the "status" key
// (the one fixed output every executor writes).
func TestAllExecutorsProvideOutputKeys(t *testing.T) {
	for _, tc := range describedExecutorCases(t) {
		t.Run(tc.nodeType, func(t *testing.T) {
			provider, ok := tc.exec.(dag.OutputKeyProvider)
			if !ok {
				t.Fatalf("executor for %q does not implement OutputKeyProvider", tc.nodeType)
			}
			keys := provider.OutputKeys()
			if len(keys) == 0 {
				t.Fatal("OutputKeys returned empty slice")
			}
			sawStatus := false
			for _, k := range keys {
				if k == "status" {
					sawStatus = true
					break
				}
			}
			if !sawStatus {
				t.Fatalf("OutputKeys missing \"status\": %v", keys)
			}
		})
	}
}
