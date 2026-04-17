package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/qmrkt/resolution-engine/dag"
)

func (*CelEvalExecutor) ConfigSchema() json.RawMessage {
	return json.RawMessage(`{
  "type": "object",
  "required": ["expressions"],
  "properties": {
    "expressions": {
      "type": "object",
      "minProperties": 1,
      "additionalProperties": {"type": "string"},
      "description": "Map of output_key -> CEL expression. Each key becomes an output under the node."
    }
  },
  "additionalProperties": false
}`)
}

func (*CelEvalExecutor) OutputKeys() []string {
	// Concrete keys come from the user's expressions map. Only status is
	// fixed. Catalog consumers should read expressions at blueprint time.
	return []string{"status"}
}

type CelEvalConfig struct {
	Expressions map[string]string `json:"expressions"` // output_key → CEL expression
}

type CelEvalExecutor struct{}

func NewCelEvalExecutor() *CelEvalExecutor {
	return &CelEvalExecutor{}
}

func (e *CelEvalExecutor) Execute(ctx context.Context, node dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	cfg, err := ParseConfig[CelEvalConfig](node.Config)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("cel_eval config: %w", err)
	}
	if len(cfg.Expressions) == 0 {
		return dag.ExecutorResult{}, fmt.Errorf("cel_eval requires at least one expression")
	}

	// Evaluate in sorted key order for deterministic output.
	keys := make([]string, 0, len(cfg.Expressions))
	for k := range cfg.Expressions {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	outputs := map[string]string{"status": "success"}
	for _, key := range keys {
		expr := strings.TrimSpace(cfg.Expressions[key])
		if expr == "" {
			return dag.ExecutorResult{}, fmt.Errorf("cel_eval expression %q is empty", key)
		}
		result, err := dag.EvalExpression(expr, inv)
		if err != nil {
			return dag.ExecutorResult{}, fmt.Errorf("cel_eval expression %q: %w", key, err)
		}
		outputs[key] = result
	}

	return dag.ExecutorResult{Outputs: outputs}, nil
}
