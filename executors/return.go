package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/qmrkt/resolution-engine/dag"
)

type ReturnConfig struct {
	Value   any    `json:"value,omitempty"`
	FromKey string `json:"from_key,omitempty"`
}

type ReturnExecutor struct{}

func NewReturnExecutor() *ReturnExecutor {
	return &ReturnExecutor{}
}

func (*ReturnExecutor) ConfigSchema() json.RawMessage {
	return json.RawMessage(`{
  "type": "object",
  "properties": {
    "value": {
      "type": "object",
      "description": "Literal JSON object to return. String leaves support {{inputs.X}} / {{results.<node>.<field>}} interpolation."
    },
    "from_key": {
      "type": "string",
      "description": "Namespaced lookup path (inputs.X / results.<node>.<field>) holding a JSON object string to return."
    }
  },
  "oneOf": [
    {"required": ["value"]},
    {"required": ["from_key"]}
  ],
  "additionalProperties": false
}`)
}

func (*ReturnExecutor) OutputKeys() []string {
	return []string{"status"}
}

// IsTerminal marks ReturnExecutor as a dag.TerminalExecutor so the engine
// can refuse to report a blueprint as "completed" if a return node was
// present but never fired. Keeps the terminal-fire contract expressed by
// the executor instead of by a hardcoded node-type string in the engine.
func (*ReturnExecutor) IsTerminal() bool { return true }

func (e *ReturnExecutor) Execute(ctx context.Context, node dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	_ = ctx
	cfg, err := ParseConfig[ReturnConfig](node.Config)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("return config: %w", err)
	}
	if err := ValidateReturnConfig(cfg); err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("return config: %w", err)
	}
	payload, err := resolveReturnPayload(cfg, inv)
	if err != nil {
		return dag.ExecutorResult{}, err
	}
	return dag.ExecutorResult{EarlyReturn: &payload}, nil
}

func ValidateReturnConfig(cfg ReturnConfig) error {
	hasValue := cfg.Value != nil
	hasFromKey := strings.TrimSpace(cfg.FromKey) != ""
	if hasValue == hasFromKey {
		return fmt.Errorf("exactly one of value or from_key is required")
	}
	if hasFromKey {
		return nil
	}
	obj, ok := cfg.Value.(map[string]any)
	if !ok {
		return fmt.Errorf("value must be a JSON object")
	}
	statusValue, ok := obj["status"]
	if !ok {
		return fmt.Errorf("value.status is required")
	}
	status, ok := statusValue.(string)
	if !ok {
		return fmt.Errorf("value.status must be a string")
	}
	if strings.TrimSpace(status) == "" {
		return fmt.Errorf("value.status must be non-empty")
	}
	return nil
}

func resolveReturnPayload(cfg ReturnConfig, inv *dag.Invocation) (json.RawMessage, error) {
	if key := strings.TrimSpace(cfg.FromKey); key != "" {
		raw := strings.TrimSpace(inv.Lookup(key))
		if raw == "" {
			return nil, fmt.Errorf("return from_key %q was empty or missing", key)
		}
		return normalizeReturnPayload(json.RawMessage(raw))
	}

	interpolated := interpolateReturnValue(cfg.Value, inv)
	raw, err := json.Marshal(interpolated)
	if err != nil {
		return nil, fmt.Errorf("marshal return value: %w", err)
	}
	return normalizeReturnPayload(raw)
}

func interpolateReturnValue(value any, inv *dag.Invocation) any {
	switch v := value.(type) {
	case string:
		return inv.Interpolate(v)
	case []interface{}:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = interpolateReturnValue(item, inv)
		}
		return out
	case map[string]interface{}:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = interpolateReturnValue(item, inv)
		}
		return out
	default:
		return value
	}
}

func normalizeReturnPayload(raw json.RawMessage) (json.RawMessage, error) {
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("return payload must be valid JSON: %w", err)
	}
	obj, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("return payload must be a JSON object")
	}
	status, ok := obj["status"].(string)
	if !ok || strings.TrimSpace(status) == "" {
		return nil, fmt.Errorf("return payload must include a non-empty status string")
	}
	normalized, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("marshal return payload: %w", err)
	}
	return json.RawMessage(normalized), nil
}
