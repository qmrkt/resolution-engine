package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

type blueprintTool struct {
	name           string
	description    string
	parameters     json.RawMessage
	inline         *dag.Blueprint
	inputMappings  map[string]string
	outputKeys     []string
	timeoutSeconds int
	engine         *dag.Engine
	parentCtx      *dag.Context
}

func (t *blueprintTool) Name() string { return t.name }
func (t *blueprintTool) Description() string {
	if strings.TrimSpace(t.description) != "" {
		return t.description
	}
	return "Run a predefined child blueprint and return selected outputs."
}
func (t *blueprintTool) Parameters() json.RawMessage { return defaultSchemaObject(t.parameters) }
func (t *blueprintTool) ReadOnly() bool              { return false }
func (t *blueprintTool) Execute(ctx context.Context, args json.RawMessage) (agentToolResult, error) {
	if t.engine == nil || t.inline == nil {
		return toolErrorResult("blueprint tool %q is not configured", t.name), nil
	}
	childInputs, err := buildBlueprintToolInputs(t.inputMappings, args, t.parentCtx)
	if err != nil {
		return toolErrorResult("%v", err), nil
	}
	childInputs["__agent_blueprint_depth"] = nextAgentBlueprintDepth(t.parentCtx)
	runCtx := ctx
	if t.timeoutSeconds > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(t.timeoutSeconds)*time.Second)
		defer cancel()
	}
	run, err := t.engine.Execute(runCtx, *t.inline, childInputs)
	if err != nil {
		output := map[string]any{"status": "failed", "error": err.Error()}
		if run != nil {
			output["run_status"] = run.Status
			output["outputs"] = selectedContextOutputs(run.Context, t.outputKeys)
		}
		return jsonToolResult(output), nil
	}
	return jsonToolResult(map[string]any{
		"status":     "success",
		"run_status": run.Status,
		"outputs":    selectedContextOutputs(run.Context, t.outputKeys),
	}), nil
}

type dynamicBlueprintTool struct {
	engine    *dag.Engine
	parentCtx *dag.Context
	policy    DynamicBlueprintPolicy
}

func (t *dynamicBlueprintTool) Name() string { return AgentBuiltinRunBlueprint }
func (t *dynamicBlueprintTool) Description() string {
	return "Run a dynamically supplied child blueprint after policy validation. Use only when predefined tools are insufficient."
}
func (t *dynamicBlueprintTool) Parameters() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"blueprint_json":{"type":"string","description":"Complete child blueprint JSON."},"inputs":{"type":"object","additionalProperties":{"type":"string"}},"output_keys":{"type":"array","items":{"type":"string"}}},"required":["blueprint_json"]}`)
}
func (t *dynamicBlueprintTool) ReadOnly() bool { return false }
func (t *dynamicBlueprintTool) Execute(ctx context.Context, args json.RawMessage) (agentToolResult, error) {
	if t.engine == nil {
		return toolErrorResult("run_blueprint is not available"), nil
	}
	var input struct {
		BlueprintJSON string            `json:"blueprint_json"`
		Inputs        map[string]string `json:"inputs"`
		OutputKeys    []string          `json:"output_keys"`
	}
	if err := json.Unmarshal(args, &input); err != nil {
		return toolErrorResult("invalid run_blueprint input: %v", err), nil
	}
	var bp dag.Blueprint
	if err := json.Unmarshal([]byte(input.BlueprintJSON), &bp); err != nil {
		return toolErrorResult("parse blueprint_json: %v", err), nil
	}
	if err := validateDynamicBlueprint(bp, t.policy, t.parentCtx); err != nil {
		return toolErrorResult("dynamic blueprint rejected: %v", err), nil
	}
	childInputs := map[string]string{}
	for k, v := range input.Inputs {
		if strings.TrimSpace(k) != "" {
			childInputs[k] = v
		}
	}
	childInputs["__agent_blueprint_depth"] = nextAgentBlueprintDepth(t.parentCtx)
	runCtx := ctx
	if t.policy.MaxTotalTimeSeconds > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(t.policy.MaxTotalTimeSeconds)*time.Second)
		defer cancel()
	}
	run, err := t.engine.Execute(runCtx, bp, childInputs)
	if err != nil {
		output := map[string]any{"status": "failed", "error": err.Error()}
		if run != nil {
			output["run_status"] = run.Status
			output["outputs"] = selectedContextOutputs(run.Context, input.OutputKeys)
		}
		return jsonToolResult(output), nil
	}
	return jsonToolResult(map[string]any{
		"status":     "success",
		"run_status": run.Status,
		"outputs":    selectedContextOutputs(run.Context, input.OutputKeys),
	}), nil
}

func buildBlueprintToolInputs(mappings map[string]string, args json.RawMessage, parent *dag.Context) (map[string]string, error) {
	var argValues map[string]any
	if len(args) > 0 && string(args) != "null" {
		if err := json.Unmarshal(args, &argValues); err != nil {
			return nil, fmt.Errorf("invalid blueprint tool arguments: %w", err)
		}
	}
	inputs := map[string]string{}
	for childKey, source := range mappings {
		childKey = strings.TrimSpace(childKey)
		source = strings.TrimSpace(source)
		if childKey == "" || source == "" {
			continue
		}
		if strings.HasPrefix(source, "$args.") {
			value, ok := argValues[strings.TrimPrefix(source, "$args.")]
			if !ok {
				inputs[childKey] = ""
				continue
			}
			inputs[childKey] = stringifyToolArg(value)
			continue
		}
		inputs[childKey] = parent.Get(source)
	}
	return inputs, nil
}

func stringifyToolArg(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(encoded)
	}
}

func selectedContextOutputs(ctx map[string]string, keys []string) map[string]string {
	outputs := map[string]string{}
	if len(keys) == 0 {
		for key, value := range ctx {
			if strings.HasPrefix(key, "__") || strings.HasPrefix(key, "input.") {
				continue
			}
			outputs[key] = value
		}
		return outputs
	}
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if value, ok := ctx[key]; ok {
			outputs[key] = value
		}
	}
	return outputs
}

func validateDynamicBlueprint(bp dag.Blueprint, policy DynamicBlueprintPolicy, parent *dag.Context) error {
	if policy.MaxNodes > 0 && len(bp.Nodes) > policy.MaxNodes {
		return fmt.Errorf("node count %d exceeds max_nodes %d", len(bp.Nodes), policy.MaxNodes)
	}
	if policy.MaxEdges > 0 && len(bp.Edges) > policy.MaxEdges {
		return fmt.Errorf("edge count %d exceeds max_edges %d", len(bp.Edges), policy.MaxEdges)
	}
	maxDepth := policy.MaxDepth
	if maxDepth <= 0 {
		maxDepth = 1
	}
	depth := parseAgentBlueprintDepth(parent.Get("__agent_blueprint_depth"))
	if depth >= maxDepth {
		return fmt.Errorf("max dynamic blueprint depth %d exceeded", maxDepth)
	}
	allowed := map[string]struct{}{}
	for _, nodeType := range policy.AllowedNodeTypes {
		nodeType = strings.TrimSpace(nodeType)
		if nodeType != "" {
			allowed[nodeType] = struct{}{}
		}
	}
	for _, node := range bp.Nodes {
		if node.Type == "agent_loop" && !policy.AllowAgentLoop {
			return fmt.Errorf("agent_loop nodes are not allowed")
		}
		if isTerminalAgentToolNode(node.Type) && !policy.AllowTerminalNodes {
			return fmt.Errorf("terminal node type %q is not allowed", node.Type)
		}
		if len(allowed) > 0 {
			if _, ok := allowed[node.Type]; !ok {
				return fmt.Errorf("node type %q is not allowed", node.Type)
			}
		}
	}
	if policy.MaxTotalTimeSeconds > 0 || policy.MaxTotalTokens > 0 {
		if bp.Budget == nil {
			bp.Budget = &dag.Budget{}
		}
		if policy.MaxTotalTimeSeconds > 0 &&
			(bp.Budget.MaxTotalTimeSeconds <= 0 || bp.Budget.MaxTotalTimeSeconds > policy.MaxTotalTimeSeconds) {
			return fmt.Errorf("blueprint budget must set max_total_time_seconds <= %d", policy.MaxTotalTimeSeconds)
		}
		if policy.MaxTotalTokens > 0 &&
			(bp.Budget.MaxTotalTokens <= 0 || bp.Budget.MaxTotalTokens > policy.MaxTotalTokens) {
			return fmt.Errorf("blueprint budget must set max_total_tokens <= %d", policy.MaxTotalTokens)
		}
	}
	if errs := dag.ValidateBlueprint(bp); len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func validateBlueprintTool(bp *dag.Blueprint) error {
	if bp == nil {
		return fmt.Errorf("inline blueprint is required")
	}
	for _, node := range bp.Nodes {
		if node.Type == "agent_loop" {
			return fmt.Errorf("inline blueprint tools cannot contain agent_loop nodes")
		}
	}
	if errs := dag.ValidateBlueprint(*bp); len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func isTerminalAgentToolNode(nodeType string) bool {
	switch nodeType {
	case "submit_result", "cancel_market", "defer_resolution":
		return true
	default:
		return false
	}
}

func parseAgentBlueprintDepth(raw string) int {
	depth, _ := strconv.Atoi(strings.TrimSpace(raw))
	if depth < 0 {
		return 0
	}
	return depth
}

func nextAgentBlueprintDepth(parent *dag.Context) string {
	return strconv.Itoa(parseAgentBlueprintDepth(parent.Get("__agent_blueprint_depth")) + 1)
}
