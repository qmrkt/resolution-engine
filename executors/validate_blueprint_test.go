package executors

import (
	"context"
	"strings"
	"testing"

	"github.com/qmrkt/resolution-engine/dag"
)

func TestValidateBlueprintExecutorAcceptsValidBlueprint(t *testing.T) {
	exec := NewValidateBlueprintExecutor(func(bp dag.Blueprint, rawJSON []byte) BlueprintValidationResult {
		if bp.ID != "valid" {
			t.Fatalf("unexpected blueprint id %q", bp.ID)
		}
		if len(rawJSON) == 0 {
			t.Fatal("expected raw blueprint json")
		}
		return BlueprintValidationResult{Valid: true, Issues: []BlueprintValidationIssue{}}
	})

	result, err := exec.Execute(context.Background(), dag.NodeDef{
		ID:   "validate",
		Type: "validate_blueprint",
		Config: map[string]any{
			"blueprint_json_key": "inputs.candidate.blueprint_json",
		},
	}, dag.NewInvocationFromInputs(map[string]string{
		"candidate.blueprint_json": `{"id":"valid","version":1,"nodes":[],"edges":[]}`,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["status"] != "success" {
		t.Fatalf("unexpected status: %+v", result.Outputs)
	}
	if result.Outputs["valid"] != "true" {
		t.Fatalf("expected valid=true, got %+v", result.Outputs)
	}
	if result.Outputs["issue_count"] != "0" {
		t.Fatalf("expected issue_count=0, got %+v", result.Outputs)
	}
	if result.Outputs["issues_text"] != "Blueprint is valid." {
		t.Fatalf("unexpected issues_text %q", result.Outputs["issues_text"])
	}
}

func TestValidateBlueprintExecutorReportsInvalidJSON(t *testing.T) {
	exec := NewValidateBlueprintExecutor(func(bp dag.Blueprint, rawJSON []byte) BlueprintValidationResult {
		t.Fatalf("validator should not run for invalid json: %+v %s", bp, string(rawJSON))
		return BlueprintValidationResult{}
	})

	result, err := exec.Execute(context.Background(), dag.NodeDef{
		ID:   "validate",
		Type: "validate_blueprint",
		Config: map[string]any{
			"blueprint_json_key": "inputs.candidate.blueprint_json",
		},
	}, dag.NewInvocationFromInputs(map[string]string{
		"candidate.blueprint_json": `{"id":`,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["valid"] != "false" {
		t.Fatalf("expected valid=false, got %+v", result.Outputs)
	}
	if result.Outputs["first_issue_code"] != "BLUEPRINT_JSON_INVALID" {
		t.Fatalf("unexpected first_issue_code %q", result.Outputs["first_issue_code"])
	}
	if !strings.Contains(result.Outputs["issues_text"], "Blueprint JSON must be valid JSON text.") {
		t.Fatalf("unexpected issues_text %q", result.Outputs["issues_text"])
	}
}

func TestValidateBlueprintExecutorSummarizesValidatorIssues(t *testing.T) {
	exec := NewValidateBlueprintExecutor(func(bp dag.Blueprint, rawJSON []byte) BlueprintValidationResult {
		return BlueprintValidationResult{
			Valid: false,
			Issues: []BlueprintValidationIssue{{
				Code:     "NO_TERMINAL_NODE",
				Message:  "Blueprint needs at least one terminal node.",
				Target:   "submit",
				Severity: "error",
			}},
		}
	})

	result, err := exec.Execute(context.Background(), dag.NodeDef{
		ID:   "validate",
		Type: "validate_blueprint",
		Config: map[string]any{
			"blueprint_json_key": "inputs.candidate.blueprint_json",
		},
	}, dag.NewInvocationFromInputs(map[string]string{
		"candidate.blueprint_json": `{"id":"candidate","version":1,"nodes":[],"edges":[]}`,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["valid"] != "false" {
		t.Fatalf("expected valid=false, got %+v", result.Outputs)
	}
	if result.Outputs["issue_count"] != "1" {
		t.Fatalf("expected issue_count=1, got %+v", result.Outputs)
	}
	if result.Outputs["first_issue_target"] != "submit" {
		t.Fatalf("expected first_issue_target=submit, got %+v", result.Outputs)
	}
	if !strings.Contains(result.Outputs["issues_text"], "[NO_TERMINAL_NODE]") {
		t.Fatalf("unexpected issues_text %q", result.Outputs["issues_text"])
	}
}
