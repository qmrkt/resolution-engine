package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/qmrkt/resolution-engine/dag"
	"github.com/qmrkt/resolution-engine/executors"
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

var terminalNodeTypes = map[string]struct{}{
	"submit_result":    {},
	"cancel_market":    {},
	"defer_resolution": {},
}

var knownNodeTypes = map[string]struct{}{
	"api_fetch":          {},
	"llm_call":           {},
	"agent_loop":         {},
	"await_signal":       {},
	"wait":               {},
	"defer_resolution":   {},
	"submit_result":      {},
	"cancel_market":      {},
	"ask_creator":        {},
	"ask_market_admin":   {},
	"cel_eval":           {},
	"map":                {},
	"gadget":             {},
	"validate_blueprint": {},
}

func ValidateResolutionBlueprint(bp dag.Blueprint, rawJSON []byte) BlueprintValidationResult {
	issues := make([]BlueprintValidationIssue, 0)
	add := func(code, message, target string) {
		issues = append(issues, BlueprintValidationIssue{Code: code, Message: message, Target: target, Severity: "error"})
	}

	if len(bp.Nodes) == 0 {
		add("EMPTY_BLUEPRINT", "Add at least one node to the blueprint.", "")
	}
	if len(bp.Nodes) > MaxBlueprintNodes {
		add("TOO_MANY_NODES", fmt.Sprintf("Blueprints are capped at %d nodes in V1.", MaxBlueprintNodes), "")
	}
	if len(rawJSON) > MaxBlueprintBytes {
		add("BLUEPRINT_TOO_LARGE", fmt.Sprintf("Resolution blueprint exceeds %dKB limit: %d bytes.", MaxBlueprintBytes/1024, len(rawJSON)), "")
	}

	nodeIDs := make(map[string]struct{}, len(bp.Nodes))
	for _, node := range bp.Nodes {
		if _, dup := nodeIDs[node.ID]; dup {
			add("DUPLICATE_NODE_ID", fmt.Sprintf("Duplicate node id %q.", node.ID), node.ID)
		}
		nodeIDs[node.ID] = struct{}{}
		if _, ok := knownNodeTypes[node.Type]; !ok {
			add("UNKNOWN_NODE_TYPE", fmt.Sprintf("Unknown node type %q.", node.Type), node.ID)
			continue
		}
		validateNodeConfig(node, &issues)
	}

	edgeIDs := make(map[string]struct{}, len(bp.Edges))
	for _, edge := range bp.Edges {
		edgeID := edge.From + "->" + edge.To
		if edge.From == edge.To {
			add("SELF_LOOP", "Self-loops are not allowed.", edgeID)
		}
		if _, dup := edgeIDs[edgeID]; dup {
			add("DUPLICATE_EDGE", fmt.Sprintf("Duplicate edge %q.", edgeID), edgeID)
		}
		edgeIDs[edgeID] = struct{}{}
		if _, ok := nodeIDs[edge.From]; !ok {
			add("DANGLING_EDGE_SOURCE", fmt.Sprintf("Edge source %q does not exist.", edge.From), edgeID)
		}
		if _, ok := nodeIDs[edge.To]; !ok {
			add("DANGLING_EDGE_TARGET", fmt.Sprintf("Edge target %q does not exist.", edge.To), edgeID)
		}
	}

	cycleResult := detectBlueprintCycles(bp)
	if cycleResult.hasCycles {
		for _, backEdgeID := range cycleResult.backEdgeIDs {
			for _, edge := range bp.Edges {
				if edge.From+"->"+edge.To == backEdgeID && edge.MaxTraversals <= 0 {
					add("BACK_EDGE_MISSING_MAX_TRAVERSALS", fmt.Sprintf("Loop edge %q must set max traversals.", backEdgeID), backEdgeID)
				}
			}
		}
	}

	roots := getRootNodes(bp)
	if len(roots) == 0 && len(bp.Nodes) > 0 {
		add("NO_ROOT", "Blueprint needs at least one root node.", "")
	}

	terminals := 0
	for _, node := range bp.Nodes {
		if isTerminalType(node.Type) {
			terminals++
		}
	}
	if terminals == 0 {
		add("NO_TERMINAL_NODE", "Blueprint needs at least one terminal node.", "")
	}

	adjacency := buildAdjacency(bp)
	reachable := make(map[string]struct{}, len(bp.Nodes))
	queue := append([]string(nil), roots...)
	for len(queue) > 0 {
		nodeID := queue[0]
		queue = queue[1:]
		if _, seen := reachable[nodeID]; seen {
			continue
		}
		reachable[nodeID] = struct{}{}
		queue = append(queue, adjacency[nodeID]...)
	}
	for _, node := range bp.Nodes {
		if len(roots) > 0 {
			if _, ok := reachable[node.ID]; !ok {
				add("UNREACHABLE_NODE", fmt.Sprintf("Node %q is unreachable from the graph roots.", displayNode(node)), node.ID)
			}
		}
	}

	outgoingByNode := make(map[string][]dag.EdgeDef, len(bp.Nodes))
	for _, node := range bp.Nodes {
		outgoingByNode[node.ID] = nil
	}
	for _, edge := range bp.Edges {
		outgoingByNode[edge.From] = append(outgoingByNode[edge.From], edge)
	}
	for _, node := range bp.Nodes {
		outgoing := outgoingByNode[node.ID]
		if isTerminalType(node.Type) {
			if len(outgoing) > 0 {
				add("TERMINAL_HAS_OUTGOING", fmt.Sprintf("Terminal node %q cannot have outgoing edges.", displayNode(node)), node.ID)
			}
		} else if len(outgoing) == 0 {
			add("NON_TERMINAL_LEAF", fmt.Sprintf("Node %q needs an outgoing path to a terminal node.", displayNode(node)), node.ID)
		}
	}

	canReachTerminal := computeReachabilityToTerminal(bp)
	for _, node := range bp.Nodes {
		if _, ok := canReachTerminal[node.ID]; !ok {
			add("NO_TERMINAL_PATH", fmt.Sprintf("Node %q does not lead to any terminal action.", displayNode(node)), node.ID)
		}
	}

	return BlueprintValidationResult{Valid: len(issues) == 0, Issues: issues}
}

type cycleDetectionResult struct {
	hasCycles   bool
	backEdgeIDs []string
}

func detectBlueprintCycles(bp dag.Blueprint) cycleDetectionResult {
	outgoing := make(map[string][]string, len(bp.Nodes))
	for _, node := range bp.Nodes {
		outgoing[node.ID] = nil
	}
	for _, edge := range bp.Edges {
		outgoing[edge.From] = append(outgoing[edge.From], edge.To)
	}
	const (
		white = 0
		gray  = 1
		black = 2
	)
	colors := make(map[string]int, len(bp.Nodes))
	for _, node := range bp.Nodes {
		colors[node.ID] = white
	}
	back := make([]string, 0)
	var dfs func(string)
	dfs = func(nodeID string) {
		colors[nodeID] = gray
		for _, next := range outgoing[nodeID] {
			switch colors[next] {
			case gray:
				back = append(back, nodeID+"->"+next)
			case white:
				dfs(next)
			}
		}
		colors[nodeID] = black
	}
	for _, node := range bp.Nodes {
		if colors[node.ID] == white {
			dfs(node.ID)
		}
	}
	return cycleDetectionResult{hasCycles: len(back) > 0, backEdgeIDs: back}
}

func getRootNodes(bp dag.Blueprint) []string {
	hasIncoming := make(map[string]struct{}, len(bp.Edges))
	for _, edge := range bp.Edges {
		hasIncoming[edge.To] = struct{}{}
	}
	roots := make([]string, 0)
	for _, node := range bp.Nodes {
		if _, ok := hasIncoming[node.ID]; !ok {
			roots = append(roots, node.ID)
		}
	}
	return roots
}

func buildAdjacency(bp dag.Blueprint) map[string][]string {
	adj := make(map[string][]string, len(bp.Nodes))
	for _, node := range bp.Nodes {
		adj[node.ID] = nil
	}
	for _, edge := range bp.Edges {
		adj[edge.From] = append(adj[edge.From], edge.To)
	}
	return adj
}

func computeReachabilityToTerminal(bp dag.Blueprint) map[string]struct{} {
	reverse := make(map[string][]string, len(bp.Nodes))
	for _, node := range bp.Nodes {
		reverse[node.ID] = nil
	}
	for _, edge := range bp.Edges {
		reverse[edge.To] = append(reverse[edge.To], edge.From)
	}
	reachable := make(map[string]struct{}, len(bp.Nodes))
	queue := make([]string, 0)
	for _, node := range bp.Nodes {
		if isTerminalType(node.Type) {
			queue = append(queue, node.ID)
		}
	}
	for len(queue) > 0 {
		nodeID := queue[0]
		queue = queue[1:]
		if _, seen := reachable[nodeID]; seen {
			continue
		}
		reachable[nodeID] = struct{}{}
		queue = append(queue, reverse[nodeID]...)
	}
	return reachable
}

func isTerminalType(nodeType string) bool {
	_, ok := terminalNodeTypes[nodeType]
	return ok
}

func displayNode(node dag.NodeDef) string {
	if strings.TrimSpace(node.Label) != "" {
		return node.Label
	}
	return node.ID
}

func validateNodeConfig(node dag.NodeDef, issues *[]BlueprintValidationIssue) {
	add := func(code, message string) {
		*issues = append(*issues, BlueprintValidationIssue{Code: code, Message: message, Target: node.ID, Severity: "error"})
	}
	switch node.Type {
	case "api_fetch":
		cfg, err := executors.ParseConfig[executors.APIFetchConfig](node.Config)
		if err != nil {
			add("API_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", displayNode(node)))
			return
		}
		if strings.TrimSpace(cfg.URL) == "" {
			add("API_URL_REQUIRED", fmt.Sprintf("Node %q needs a URL.", displayNode(node)))
		}
		if strings.TrimSpace(cfg.URL) != "" {
			if _, err := url.ParseRequestURI(cfg.URL); err != nil || !strings.Contains(cfg.URL, "://") {
				add("API_URL_INVALID", fmt.Sprintf("Node %q needs a valid absolute URL.", displayNode(node)))
			}
		}
		if strings.TrimSpace(cfg.Method) != "" {
			if _, err := executors.NormalizeAPIFetchMethod(cfg.Method); err != nil {
				add("API_METHOD_INVALID", fmt.Sprintf("Node %q uses an unsupported HTTP method.", displayNode(node)))
			}
		}
		if cfg.TimeoutSeconds < 0 {
			add("API_TIMEOUT_INVALID", fmt.Sprintf("Node %q needs a non-negative timeout.", displayNode(node)))
		}
	case "llm_call":
		cfg, err := executors.ParseConfig[executors.LLMCallConfig](node.Config)
		if err != nil {
			add("LLM_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", displayNode(node)))
			return
		}
		if strings.TrimSpace(cfg.Prompt) == "" {
			add("LLM_PROMPT_REQUIRED", fmt.Sprintf("Node %q needs a prompt.", displayNode(node)))
		}
		if cfg.AllowedOutcomesKey != "" && strings.TrimSpace(cfg.AllowedOutcomesKey) == "" {
			add("LLM_ALLOWED_OUTCOMES_KEY_INVALID", fmt.Sprintf("Node %q has an empty allowed outcomes key.", displayNode(node)))
		}
	case "agent_loop":
		cfg, err := executors.ParseConfig[executors.AgentLoopConfig](node.Config)
		if err != nil {
			add("AGENT_LOOP_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", displayNode(node)))
			return
		}
		if strings.TrimSpace(cfg.Prompt) == "" {
			add("AGENT_LOOP_PROMPT_REQUIRED", fmt.Sprintf("Node %q needs a prompt.", displayNode(node)))
		}
		outputMode := strings.ToLower(strings.TrimSpace(cfg.OutputMode))
		if outputMode == "" {
			outputMode = executors.AgentOutputModeText
		}
		switch outputMode {
		case executors.AgentOutputModeText, executors.AgentOutputModeStructured, executors.AgentOutputModeResolution:
		default:
			add("AGENT_LOOP_OUTPUT_MODE_INVALID", fmt.Sprintf("Node %q has an unsupported output_mode.", displayNode(node)))
		}
		if outputMode == executors.AgentOutputModeStructured {
			if cfg.OutputTool == nil || len(cfg.OutputTool.Parameters) == 0 {
				add("AGENT_LOOP_OUTPUT_TOOL_REQUIRED", fmt.Sprintf("Node %q structured output needs output_tool.parameters.", displayNode(node)))
			}
		}
		if cfg.OutputTool != nil && len(cfg.OutputTool.Parameters) > 0 && !json.Valid(cfg.OutputTool.Parameters) {
			add("AGENT_LOOP_OUTPUT_TOOL_INVALID", fmt.Sprintf("Node %q output_tool.parameters must be valid JSON.", displayNode(node)))
		}
		if cfg.TimeoutSeconds < 0 || cfg.ToolTimeoutSeconds < 0 {
			add("AGENT_LOOP_TIMEOUT_INVALID", fmt.Sprintf("Node %q needs non-negative timeouts.", displayNode(node)))
		}
		if cfg.MaxSteps < 0 || cfg.MaxToolCalls < 0 || cfg.MaxToolResultBytes < 0 || cfg.MaxTokens < 0 {
			add("AGENT_LOOP_LIMIT_INVALID", fmt.Sprintf("Node %q needs non-negative limits.", displayNode(node)))
		}
		if cfg.AllowedOutcomesKey != "" && strings.TrimSpace(cfg.AllowedOutcomesKey) == "" {
			add("AGENT_LOOP_ALLOWED_OUTCOMES_KEY_INVALID", fmt.Sprintf("Node %q has an empty allowed outcomes key.", displayNode(node)))
		}
		validateAgentLoopTools(node, cfg, add)
	case "await_signal":
		cfg, err := executors.ParseConfig[awaitSignalConfig](node.Config)
		if err != nil {
			add("AWAIT_SIGNAL_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", displayNode(node)))
			return
		}
		if strings.TrimSpace(cfg.SignalType) == "" {
			add("AWAIT_SIGNAL_TYPE_REQUIRED", fmt.Sprintf("Node %q needs a signal_type.", displayNode(node)))
		}
		if cfg.TimeoutSeconds < 0 || cfg.TimeoutSeconds > 604800 {
			add("AWAIT_SIGNAL_TIMEOUT_INVALID", fmt.Sprintf("Node %q needs a timeout between 0 and 604800 seconds.", displayNode(node)))
		}
		for _, key := range cfg.RequiredPayload {
			if strings.TrimSpace(key) == "" {
				add("AWAIT_SIGNAL_REQUIRED_PAYLOAD_INVALID", fmt.Sprintf("Node %q has an empty required payload key.", displayNode(node)))
			}
		}
	case "wait":
		cfg, err := executors.ParseConfig[executors.WaitConfig](node.Config)
		if err != nil {
			add("WAIT_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", displayNode(node)))
			return
		}
		if cfg.DurationSeconds < 0 {
			add("WAIT_DURATION_INVALID", fmt.Sprintf("Node %q needs a non-negative wait duration.", displayNode(node)))
		}
		if cfg.Mode != "" && cfg.Mode != "sleep" && cfg.Mode != "defer" {
			add("WAIT_MODE_INVALID", fmt.Sprintf("Node %q must use either sleep or defer mode.", displayNode(node)))
		}
		if cfg.StartFrom != "" && cfg.StartFrom != "now" && cfg.StartFrom != "deadline" && cfg.StartFrom != "resolution_pending_since" {
			add("WAIT_START_FROM_INVALID", fmt.Sprintf("Node %q has an unsupported wait anchor.", displayNode(node)))
		}
		if cfg.Mode == "defer" && cfg.StartFrom == "now" {
			add("WAIT_DEFER_START_NOW_UNSUPPORTED", fmt.Sprintf("Node %q cannot use \"now\" with defer mode.", displayNode(node)))
		}
	case "submit_result":
		cfg, err := executors.ParseConfig[executors.SubmitResultConfig](node.Config)
		if err != nil {
			add("SUBMIT_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", displayNode(node)))
			return
		}
		if strings.TrimSpace(cfg.OutcomeKey) == "" {
			add("SUBMIT_OUTCOME_KEY_REQUIRED", fmt.Sprintf("Node %q needs an outcome source.", displayNode(node)))
		}
	case "cel_eval":
		cfg, err := executors.ParseConfig[executors.CelEvalConfig](node.Config)
		if err != nil {
			add("CEL_EVAL_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", displayNode(node)))
			return
		}
		if len(cfg.Expressions) == 0 {
			add("CEL_EVAL_EXPRESSIONS_REQUIRED", fmt.Sprintf("Node %q needs at least one expression.", displayNode(node)))
		}
	case "map":
		if field, ok, err := executors.DeprecatedMapConfigField(node.Config); err != nil {
			add("MAP_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", displayNode(node)))
			return
		} else if ok {
			add("MAP_FIELD_REMOVED", fmt.Sprintf("Node %q uses removed map field %q; use batch_size and max_concurrency.", displayNode(node), field))
		}
		cfg, err := executors.ParseConfig[executors.MapConfig](node.Config)
		if err != nil {
			add("MAP_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", displayNode(node)))
			return
		}
		if strings.TrimSpace(cfg.ItemsKey) == "" {
			add("MAP_ITEMS_KEY_REQUIRED", fmt.Sprintf("Node %q needs an items_key.", displayNode(node)))
		}
		if cfg.Inline == nil || len(cfg.Inline.Nodes) == 0 {
			add("MAP_INLINE_REQUIRED", fmt.Sprintf("Node %q needs an inline blueprint.", displayNode(node)))
		}
		if cfg.Inline != nil {
			if errs := dag.ValidateBlueprint(*cfg.Inline); len(errs) > 0 {
				add("MAP_INLINE_INVALID", fmt.Sprintf("Node %q inline blueprint: %v", displayNode(node), errs[0]))
			}
		}
		if cfg.BatchSize < 0 {
			add("MAP_BATCH_SIZE_INVALID", fmt.Sprintf("Node %q batch_size must be non-negative.", displayNode(node)))
		}
		if cfg.MaxConcurrency != nil && *cfg.MaxConcurrency < 0 {
			add("MAP_MAX_CONCURRENCY_INVALID", fmt.Sprintf("Node %q max_concurrency must be non-negative.", displayNode(node)))
		}
		if cfg.MaxItems < 0 {
			add("MAP_MAX_ITEMS_INVALID", fmt.Sprintf("Node %q max_items must be non-negative.", displayNode(node)))
		}
		if cfg.MaxDepth < 0 {
			add("MAP_MAX_DEPTH_INVALID", fmt.Sprintf("Node %q max_depth must be non-negative.", displayNode(node)))
		}
		if cfg.PerBatchTimeoutSeconds < 0 {
			add("MAP_TIMEOUT_INVALID", fmt.Sprintf("Node %q per_batch_timeout_seconds must be non-negative.", displayNode(node)))
		}
		onError := strings.TrimSpace(cfg.OnError)
		if onError != "" && onError != "fail" && onError != "continue" {
			add("MAP_ON_ERROR_INVALID", fmt.Sprintf("Node %q on_error must be \"fail\" or \"continue\".", displayNode(node)))
		}
	case "gadget":
		cfg, err := executors.ParseConfig[executors.GadgetConfig](node.Config)
		if err != nil {
			add("GADGET_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", displayNode(node)))
			return
		}
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
		switch {
		case sourceCount == 0:
			add("GADGET_BLUEPRINT_SOURCE_REQUIRED", fmt.Sprintf("Node %q needs exactly one of blueprint_json, blueprint_json_key, or inline.", displayNode(node)))
		case sourceCount > 1:
			add("GADGET_BLUEPRINT_SOURCE_CONFLICT", fmt.Sprintf("Node %q can use only one of blueprint_json, blueprint_json_key, or inline.", displayNode(node)))
		}
		if cfg.TimeoutSeconds < 0 {
			add("GADGET_TIMEOUT_INVALID", fmt.Sprintf("Node %q timeout_seconds must be non-negative.", displayNode(node)))
		}
		if cfg.MaxDepth < 0 {
			add("GADGET_MAX_DEPTH_INVALID", fmt.Sprintf("Node %q max_depth must be non-negative.", displayNode(node)))
		}
		if policy := cfg.DynamicBlueprintPolicy; policy != nil {
			if policy.MaxNodes < 0 ||
				policy.MaxEdges < 0 ||
				policy.MaxDepth < 0 ||
				policy.MaxTotalTimeSeconds < 0 ||
				policy.MaxTotalTokens < 0 {
				add("GADGET_DYNAMIC_POLICY_INVALID", fmt.Sprintf("Node %q dynamic blueprint policy limits must be non-negative.", displayNode(node)))
			}
		}
		if cfg.Inline != nil {
			rawInline, err := json.Marshal(cfg.Inline)
			if err != nil {
				add("GADGET_INLINE_INVALID", fmt.Sprintf("Node %q inline blueprint could not be serialized.", displayNode(node)))
			} else if child := ValidateResolutionBlueprint(*cfg.Inline, rawInline); !child.Valid {
				first := child.Issues[0]
				add("GADGET_INLINE_INVALID", fmt.Sprintf("Node %q inline blueprint is invalid: %s", displayNode(node), strings.TrimSpace(first.Message)))
			}
		}
		if raw := strings.TrimSpace(cfg.BlueprintJSON); raw != "" && !strings.Contains(raw, "{{") {
			if !json.Valid([]byte(raw)) {
				add("GADGET_BLUEPRINT_JSON_INVALID", fmt.Sprintf("Node %q blueprint_json must be valid JSON text.", displayNode(node)))
			} else {
				var child dag.Blueprint
				if err := json.Unmarshal([]byte(raw), &child); err != nil {
					add("GADGET_BLUEPRINT_JSON_INVALID", fmt.Sprintf("Node %q blueprint_json could not be parsed.", displayNode(node)))
				} else if result := ValidateResolutionBlueprint(child, []byte(raw)); !result.Valid {
					first := result.Issues[0]
					add("GADGET_BLUEPRINT_INVALID", fmt.Sprintf("Node %q blueprint_json is invalid: %s", displayNode(node), strings.TrimSpace(first.Message)))
				}
			}
		}
	case "validate_blueprint":
		cfg, err := executors.ParseConfig[executors.ValidateBlueprintConfig](node.Config)
		if err != nil {
			add("VALIDATE_BLUEPRINT_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", displayNode(node)))
			return
		}
		if strings.TrimSpace(cfg.BlueprintJSONKey) == "" {
			add("VALIDATE_BLUEPRINT_KEY_REQUIRED", fmt.Sprintf("Node %q needs a blueprint_json_key.", displayNode(node)))
		}
	}
}

func validateAgentLoopTools(node dag.NodeDef, cfg executors.AgentLoopConfig, add func(code, message string)) {
	validBuiltins := map[string]struct{}{
		executors.AgentBuiltinContextGet:   {},
		executors.AgentBuiltinContextList:  {},
		executors.AgentBuiltinSourceFetch:  {},
		executors.AgentBuiltinJSONExtract:  {},
		executors.AgentBuiltinRunBlueprint: {},
	}
	for _, tool := range cfg.Tools {
		name := strings.TrimSpace(tool.Name)
		if name == "" {
			add("AGENT_LOOP_TOOL_NAME_REQUIRED", fmt.Sprintf("Node %q has an agent tool without a name.", displayNode(node)))
		}
		if len(tool.Parameters) > 0 && !json.Valid(tool.Parameters) {
			add("AGENT_LOOP_TOOL_PARAMETERS_INVALID", fmt.Sprintf("Node %q tool %q parameters must be valid JSON.", displayNode(node), name))
		}
		if tool.TimeoutSeconds < 0 {
			add("AGENT_LOOP_TOOL_TIMEOUT_INVALID", fmt.Sprintf("Node %q tool %q needs a non-negative timeout.", displayNode(node), name))
		}
		if tool.MaxDepth < 0 {
			add("AGENT_LOOP_TOOL_MAX_DEPTH_INVALID", fmt.Sprintf("Node %q tool %q needs a non-negative max_depth.", displayNode(node), name))
		}

		kind := strings.ToLower(strings.TrimSpace(tool.Kind))
		if kind == "" {
			if tool.Inline != nil {
				kind = executors.AgentToolKindBlueprint
			} else {
				kind = executors.AgentToolKindBuiltin
			}
		}
		switch kind {
		case executors.AgentToolKindBuiltin:
			builtin := strings.TrimSpace(tool.Builtin)
			if builtin == "" {
				builtin = name
			}
			if _, ok := validBuiltins[builtin]; !ok {
				add("AGENT_LOOP_BUILTIN_TOOL_INVALID", fmt.Sprintf("Node %q uses unknown builtin tool %q.", displayNode(node), builtin))
			}
			if builtin == executors.AgentBuiltinRunBlueprint && !cfg.EnableDynamicBlueprint {
				add("AGENT_LOOP_DYNAMIC_BLUEPRINT_DISABLED", fmt.Sprintf("Node %q tool %q requires enable_dynamic_blueprints.", displayNode(node), name))
			}
		case executors.AgentToolKindBlueprint:
			if tool.Inline == nil || len(tool.Inline.Nodes) == 0 {
				add("AGENT_LOOP_BLUEPRINT_TOOL_INLINE_REQUIRED", fmt.Sprintf("Node %q tool %q needs an inline blueprint.", displayNode(node), name))
				continue
			}
			if errs := dag.ValidateBlueprint(*tool.Inline); len(errs) > 0 {
				add("AGENT_LOOP_BLUEPRINT_TOOL_INLINE_INVALID", fmt.Sprintf("Node %q tool %q inline blueprint: %v.", displayNode(node), name, errs[0]))
			}
		default:
			add("AGENT_LOOP_TOOL_KIND_INVALID", fmt.Sprintf("Node %q tool %q has unsupported kind %q.", displayNode(node), name, tool.Kind))
		}
	}

	if policy := cfg.DynamicBlueprintPolicy; policy != nil {
		if policy.MaxNodes < 0 ||
			policy.MaxEdges < 0 ||
			policy.MaxDepth < 0 ||
			policy.MaxTotalTimeSeconds < 0 ||
			policy.MaxTotalTokens < 0 {
			add("AGENT_LOOP_DYNAMIC_POLICY_INVALID", fmt.Sprintf("Node %q dynamic blueprint policy limits must be non-negative.", displayNode(node)))
		}
	}
}
