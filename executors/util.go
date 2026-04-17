package executors

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// ParseConfig unmarshals a node's config into a typed struct.
func ParseConfig[T any](raw interface{}) (T, error) {
	var cfg T
	data, err := json.Marshal(raw)
	if err != nil {
		return cfg, fmt.Errorf("marshal config: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("unmarshal config: %w", err)
	}
	return cfg, nil
}

func cloneRawMessage(value json.RawMessage) json.RawMessage {
	if len(value) == 0 {
		return nil
	}
	return append(json.RawMessage(nil), value...)
}

// defaultString returns value when non-empty (after trimming), otherwise
// fallback. Used by config readers that accept optional fields with a
// documented default.
func defaultString(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
