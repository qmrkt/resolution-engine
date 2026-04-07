package executors

import (
	"encoding/json"
	"fmt"
)

// parseConfig unmarshals a node's config into a typed struct.
func parseConfig[T any](raw interface{}) (T, error) {
	var cfg T
	switch v := raw.(type) {
	case map[string]interface{}:
		data, err := json.Marshal(v)
		if err != nil {
			return cfg, fmt.Errorf("marshal config: %w", err)
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
			return cfg, fmt.Errorf("unmarshal config: %w", err)
		}
	default:
		data, err := json.Marshal(raw)
		if err != nil {
			return cfg, fmt.Errorf("marshal config: %w", err)
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
			return cfg, fmt.Errorf("unmarshal config: %w", err)
		}
	}
	return cfg, nil
}
