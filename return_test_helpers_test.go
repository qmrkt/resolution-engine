package main

import (
	"encoding/json"
	"testing"
)

func returnObject(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	if len(raw) == 0 {
		t.Fatal("expected non-empty return payload")
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal return payload: %v", err)
	}
	return decoded
}

func returnStringField(t *testing.T, raw json.RawMessage, field string) string {
	t.Helper()
	value, _ := returnObject(t, raw)[field].(string)
	return value
}
