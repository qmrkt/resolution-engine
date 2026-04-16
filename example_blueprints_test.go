package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/qmrkt/resolution-engine/dag"
)

func TestYoloAutoResolutionExampleBlueprintIsValid(t *testing.T) {
	raw, err := os.ReadFile("docs/examples/yolo-auto-resolution/blueprint.json")
	if err != nil {
		t.Fatalf("read example blueprint: %v", err)
	}

	var bp dag.Blueprint
	if err := json.Unmarshal(raw, &bp); err != nil {
		t.Fatalf("unmarshal example blueprint: %v", err)
	}

	result := ValidateResolutionBlueprint(bp, raw)
	if !result.Valid {
		t.Fatalf("expected yolo example blueprint to be valid, got issues: %+v", result.Issues)
	}
}
