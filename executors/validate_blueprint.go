package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/qmrkt/resolution-engine/dag"
)

type BlueprintValidationIssue struct {
	Code     string `json:"code"`
	Message  string `json:"message"`
	Target   string `json:"target,omitempty"`
	Severity string `json:"severity"`
}

type BlueprintValidationResult struct {
	Valid  bool                       `json:"valid"`
	Issues []BlueprintValidationIssue `json:"issues"`
}

type BlueprintValidatorFunc func(bp dag.Blueprint, rawJSON []byte) BlueprintValidationResult

type ValidateBlueprintConfig struct {
	BlueprintJSONKey string `json:"blueprint_json_key"`
}

type ValidateBlueprintExecutor struct {
	Validate BlueprintValidatorFunc
}

func NewValidateBlueprintExecutor(validate BlueprintValidatorFunc) *ValidateBlueprintExecutor {
	return &ValidateBlueprintExecutor{Validate: validate}
}

func (e *ValidateBlueprintExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	_ = ctx
	if e == nil || e.Validate == nil {
		return dag.ExecutorResult{}, fmt.Errorf("validate_blueprint executor is not configured")
	}

	cfg, err := ParseConfig[ValidateBlueprintConfig](node.Config)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("validate_blueprint config: %w", err)
	}
	key := strings.TrimSpace(cfg.BlueprintJSONKey)
	if key == "" {
		return dag.ExecutorResult{}, fmt.Errorf("validate_blueprint config: blueprint_json_key is required")
	}

	raw := execCtx.Get(key)
	result := validateBlueprintInput(raw, e.Validate)
	issuesJSON, err := json.Marshal(result.Issues)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("validate_blueprint: marshal issues: %w", err)
	}

	outputs := map[string]string{
		"status":         "success",
		"valid":          strconv.FormatBool(result.Valid),
		"issue_count":    strconv.Itoa(len(result.Issues)),
		"issues_json":    string(issuesJSON),
		"issues_text":    summarizeBlueprintIssues(result.Issues),
		"blueprint_json": raw,
	}
	if len(result.Issues) > 0 {
		outputs["first_issue_code"] = result.Issues[0].Code
		outputs["first_issue_message"] = result.Issues[0].Message
		outputs["first_issue_target"] = result.Issues[0].Target
	}

	return dag.ExecutorResult{Outputs: outputs}, nil
}

func validateBlueprintInput(raw string, validate BlueprintValidatorFunc) BlueprintValidationResult {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return BlueprintValidationResult{
			Valid: false,
			Issues: []BlueprintValidationIssue{{
				Code:     "BLUEPRINT_JSON_REQUIRED",
				Message:  "Provide a blueprint JSON string to validate.",
				Severity: "error",
			}},
		}
	}
	if !json.Valid([]byte(trimmed)) {
		return BlueprintValidationResult{
			Valid: false,
			Issues: []BlueprintValidationIssue{{
				Code:     "BLUEPRINT_JSON_INVALID",
				Message:  "Blueprint JSON must be valid JSON text.",
				Severity: "error",
			}},
		}
	}

	var bp dag.Blueprint
	if err := json.Unmarshal([]byte(trimmed), &bp); err != nil {
		return BlueprintValidationResult{
			Valid: false,
			Issues: []BlueprintValidationIssue{{
				Code:     "BLUEPRINT_JSON_INVALID",
				Message:  fmt.Sprintf("Blueprint JSON could not be parsed: %v", err),
				Severity: "error",
			}},
		}
	}

	result := validate(bp, []byte(trimmed))
	if result.Issues == nil {
		result.Issues = []BlueprintValidationIssue{}
	}
	return result
}

func summarizeBlueprintIssues(issues []BlueprintValidationIssue) string {
	if len(issues) == 0 {
		return "Blueprint is valid."
	}
	lines := make([]string, 0, len(issues))
	for idx, issue := range issues {
		prefix := fmt.Sprintf("%d. [%s] %s", idx+1, defaultString(strings.TrimSpace(issue.Code), "UNKNOWN"), strings.TrimSpace(issue.Message))
		if target := strings.TrimSpace(issue.Target); target != "" {
			prefix += " (target: " + target + ")"
		}
		lines = append(lines, prefix)
	}
	return strings.Join(lines, "\n")
}
