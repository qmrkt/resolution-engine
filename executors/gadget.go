package executors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/qmrkt/resolution-engine/dag"
)

const (
	gadgetDepthKey        = "__gadget_depth"
	defaultGadgetMaxDepth = 1
)

// defaultGadgetAllowedNodeTypes lists node types permitted in a gadget child
// by default. Short inline waits are allowed because they complete
// synchronously, while suspension-capable types such as await_signal and async
// agent_loop are blocked by ValidateNoSuspensionCapableNodes before execution.
var defaultGadgetAllowedNodeTypes = []string{
	"api_fetch",
	"llm_call",
	"agent_loop",
	"wait",
	"return",
	"cel_eval",
	"map",
}

type GadgetConfig struct {
	BlueprintJSON          string                  `json:"blueprint_json,omitempty"`
	BlueprintJSONKey       string                  `json:"blueprint_json_key,omitempty"`
	Inline                 *dag.Blueprint          `json:"inline,omitempty"`
	InputMappings          map[string]string       `json:"input_mappings,omitempty"`
	TimeoutSeconds         int                     `json:"timeout_seconds,omitempty"`
	MaxDepth               int                     `json:"max_depth,omitempty"`
	DynamicBlueprintPolicy *DynamicBlueprintPolicy `json:"dynamic_blueprint_policy,omitempty"`
}

type GadgetExecutor struct {
	engine   *dag.Engine
	validate BlueprintValidatorFunc
}

func NewGadgetExecutor(engine *dag.Engine, validate BlueprintValidatorFunc) *GadgetExecutor {
	return &GadgetExecutor{engine: engine, validate: validate}
}

func (*GadgetExecutor) ConfigSchema() json.RawMessage {
	return json.RawMessage(`{
  "type": "object",
  "properties": {
    "blueprint_json": {"type": "string", "description": "Child blueprint as a JSON string. Mutually exclusive with blueprint_json_key and inline."},
    "blueprint_json_key": {"type": "string", "description": "Namespaced lookup path (inputs.X / results.<node>.<field>) holding a child blueprint JSON string."},
    "inline": {"type": "object", "description": "Child blueprint as a structured object."},
    "input_mappings": {"type": "object", "additionalProperties": {"type": "string"}, "description": "child_input_key -> parent_context_key."},
    "timeout_seconds": {"type": "integer", "minimum": 0},
    "max_depth": {"type": "integer", "minimum": 0, "description": "Recursion guard across nested gadgets. Defaults to 1."},
    "dynamic_blueprint_policy": {"type": "object", "description": "Overrides the default allowed-node-types, depth, and token budgets for runtime-supplied child blueprints."}
  },
  "oneOf": [
    {"required": ["blueprint_json"]},
    {"required": ["blueprint_json_key"]},
    {"required": ["inline"]}
  ],
  "additionalProperties": false
}`)
}

func (*GadgetExecutor) OutputKeys() []string {
	return []string{"status", "error", "run_status", "child_run_id", "return_json"}
}

func (e *GadgetExecutor) Execute(ctx context.Context, node dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	if e == nil || e.engine == nil {
		return dag.ExecutorResult{}, fmt.Errorf("gadget executor is not configured")
	}

	cfg, err := ParseConfig[GadgetConfig](node.Config)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("gadget config: %w", err)
	}
	if err := validateGadgetConfig(cfg); err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("gadget config: %w", err)
	}

	maxDepth := cfg.MaxDepth
	if maxDepth <= 0 {
		maxDepth = defaultGadgetMaxDepth
	}
	currentDepth, err := parseDepthCounter(inv.Run.Inputs[gadgetDepthKey])
	if err != nil {
		return dag.ExecutorResult{Outputs: map[string]string{
			"status": "failed",
			"error":  fmt.Sprintf("gadget depth: %v", err),
		}}, nil
	}
	if currentDepth >= maxDepth {
		return dag.ExecutorResult{Outputs: map[string]string{
			"status": "failed",
			"error":  fmt.Sprintf("gadget exceeded max_depth %d", maxDepth),
		}}, nil
	}

	childBlueprint, rawJSON, failure := resolveGadgetBlueprint(inv, cfg)
	if failure != nil {
		return dag.ExecutorResult{Outputs: map[string]string{
			"status": "failed",
			"error":  failure.Error(),
		}}, nil
	}

	if validationOutputs, ok := e.validateChildBlueprint(childBlueprint, rawJSON, cfg, currentDepth); ok {
		return dag.ExecutorResult{Outputs: validationOutputs}, nil
	}

	childInputs := make(map[string]string, len(cfg.InputMappings)+1)
	for childKey, parentKey := range cfg.InputMappings {
		childKey = strings.TrimSpace(childKey)
		parentKey = strings.TrimSpace(parentKey)
		if childKey == "" || parentKey == "" {
			continue
		}
		childInputs[childKey] = inv.Lookup(parentKey)
	}
	childInputs[gadgetDepthKey] = strconv.Itoa(currentDepth + 1)

	runCtx := ctx
	if cfg.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(cfg.TimeoutSeconds)*time.Second)
		defer cancel()
	}

	run, err := e.engine.Execute(runCtx, childBlueprint, childInputs)
	usage := dag.TokenUsage{}
	if run != nil {
		usage = run.Usage
	}
	if err != nil {
		if errors.Is(err, context.Canceled) && ctx != nil && ctx.Err() != nil {
			return dag.ExecutorResult{}, err
		}
		outputs := gadgetRunOutputs(run)
		outputs["status"] = "failed"
		outputs["error"] = err.Error()
		return dag.ExecutorResult{Outputs: outputs, Usage: usage}, nil
	}

	outputs := gadgetRunOutputs(run)
	if run != nil && run.Status == "failed" {
		outputs["status"] = "failed"
		if strings.TrimSpace(outputs["error"]) == "" {
			outputs["error"] = defaultString(strings.TrimSpace(run.Error), "child blueprint execution failed")
		}
		return dag.ExecutorResult{Outputs: outputs, Usage: usage}, nil
	}
	outputs["status"] = "success"
	return dag.ExecutorResult{Outputs: outputs, Usage: usage}, nil
}

func validateGadgetConfig(cfg GadgetConfig) error {
	sourceCount := 0
	if strings.TrimSpace(cfg.BlueprintJSON) != "" {
		sourceCount++
	}
	if strings.TrimSpace(cfg.BlueprintJSONKey) != "" {
		sourceCount++
	}
	if cfg.Inline != nil {
		sourceCount++
	}
	if sourceCount == 0 {
		return fmt.Errorf("one of blueprint_json, blueprint_json_key, or inline is required")
	}
	if sourceCount > 1 {
		return fmt.Errorf("only one of blueprint_json, blueprint_json_key, or inline may be set")
	}
	if cfg.TimeoutSeconds < 0 {
		return fmt.Errorf("timeout_seconds must be non-negative")
	}
	if cfg.MaxDepth < 0 {
		return fmt.Errorf("max_depth must be non-negative")
	}
	if policy := cfg.DynamicBlueprintPolicy; policy != nil {
		if policy.MaxNodes < 0 ||
			policy.MaxEdges < 0 ||
			policy.MaxDepth < 0 ||
			policy.MaxTotalTimeSeconds < 0 ||
			policy.MaxTotalTokens < 0 {
			return fmt.Errorf("dynamic_blueprint_policy limits must be non-negative")
		}
	}
	return nil
}

func resolveGadgetBlueprint(inv *dag.Invocation, cfg GadgetConfig) (dag.Blueprint, []byte, error) {
	if cfg.Inline != nil {
		rawJSON, err := json.Marshal(cfg.Inline)
		if err != nil {
			return dag.Blueprint{}, nil, fmt.Errorf("marshal inline blueprint: %w", err)
		}
		return *cfg.Inline, rawJSON, nil
	}

	raw := strings.TrimSpace(cfg.BlueprintJSON)
	if key := strings.TrimSpace(cfg.BlueprintJSONKey); key != "" {
		raw = strings.TrimSpace(inv.Lookup(key))
		if raw == "" {
			return dag.Blueprint{}, nil, fmt.Errorf("blueprint_json_key %q was empty or missing", key)
		}
	}
	if raw == "" {
		return dag.Blueprint{}, nil, fmt.Errorf("blueprint_json was empty")
	}
	rawBytes := []byte(raw)
	var bp dag.Blueprint
	if err := json.Unmarshal(rawBytes, &bp); err != nil {
		return dag.Blueprint{}, nil, fmt.Errorf("blueprint_json must be valid JSON text")
	}
	return bp, rawBytes, nil
}

func (e *GadgetExecutor) validateChildBlueprint(bp dag.Blueprint, rawJSON []byte, cfg GadgetConfig, currentDepth int) (map[string]string, bool) {
	if e != nil && e.validate != nil {
		validation := e.validate(bp, rawJSON)
		if !validation.Valid {
			issuesJSON, _ := json.Marshal(validation.Issues)
			outputs := map[string]string{
				"status":      "failed",
				"error":       "child blueprint validation failed",
				"valid":       "false",
				"issue_count": strconv.Itoa(len(validation.Issues)),
				"issues_json": string(issuesJSON),
				"issues_text": summarizeBlueprintIssues(validation.Issues),
			}
			if len(validation.Issues) > 0 {
				outputs["first_issue_code"] = validation.Issues[0].Code
				outputs["first_issue_message"] = validation.Issues[0].Message
				outputs["first_issue_target"] = validation.Issues[0].Target
			}
			return outputs, true
		}
	}

	policy := defaultGadgetBlueprintPolicy(cfg.DynamicBlueprintPolicy)
	if policy.MaxDepth > 0 && currentDepth >= policy.MaxDepth {
		return map[string]string{
			"status": "failed",
			"error":  fmt.Sprintf("child blueprint rejected: max dynamic blueprint depth %d exceeded", policy.MaxDepth),
		}, true
	}
	if err := validateRuntimeBlueprintStructure(bp, policy); err != nil {
		return map[string]string{
			"status": "failed",
			"error":  fmt.Sprintf("child blueprint rejected: %v", err),
		}, true
	}
	if err := e.engine.ValidateNoSuspensionCapableNodes(bp); err != nil {
		return map[string]string{
			"status": "failed",
			"error":  fmt.Sprintf("child blueprint rejected: %v", err),
		}, true
	}
	return nil, false
}

func defaultGadgetBlueprintPolicy(policy *DynamicBlueprintPolicy) DynamicBlueprintPolicy {
	if policy == nil {
		return DynamicBlueprintPolicy{
			AllowedNodeTypes:    append([]string(nil), defaultGadgetAllowedNodeTypes...),
			MaxNodes:            16,
			MaxEdges:            24,
			MaxTotalTimeSeconds: 300,
			MaxTotalTokens:      120000,
			AllowAgentLoop:      true,
		}
	}
	return *policy
}

func gadgetRunOutputs(run *dag.RunState) map[string]string {
	outputs := map[string]string{}
	if run == nil {
		return outputs
	}
	outputs["run_status"] = run.Status
	outputs["child_run_id"] = run.ID
	if len(run.Return) > 0 {
		outputs["return_json"] = string(run.Return)
	}
	return outputs
}

func parseDepthCounter(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	depth, err := strconv.Atoi(raw)
	if err != nil {
		return 0, err
	}
	if depth < 0 {
		return 0, fmt.Errorf("negative depth %d", depth)
	}
	return depth, nil
}
