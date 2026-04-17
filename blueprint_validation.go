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

var knownNodeTypes = map[string]struct{}{
	"api_fetch":          {},
	"llm_call":           {},
	"agent_loop":         {},
	"await_signal":       {},
	"wait":               {},
	"return":             {},
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

	scheduler := dag.NewScheduler(bp)
	for _, edge := range bp.Edges {
		if scheduler.IsBackEdge(edge.From, edge.To) && edge.MaxTraversals <= 0 {
			edgeID := edge.From + "->" + edge.To
			add("BACK_EDGE_MISSING_MAX_TRAVERSALS", fmt.Sprintf("Loop edge %q must set max traversals.", edgeID), edgeID)
		}
	}

	for _, issue := range executors.ValidateReturnContract(bp) {
		issues = append(issues, BlueprintValidationIssue{
			Code:     issue.Code,
			Message:  issue.Message,
			Target:   issue.Target,
			Severity: issue.Severity,
		})
	}

	validateEdgeReferences(bp, &issues)

	valid := true
	for _, issue := range issues {
		if strings.TrimSpace(issue.Severity) != "warning" {
			valid = false
			break
		}
	}
	return BlueprintValidationResult{Valid: valid, Issues: issues}
}

// validateEdgeReferences scans each edge condition for identifiers of the
// form `nodeID.key` and flags references to output keys the target node's
// executor does not declare. Adds a typo suggestion (Levenshtein-closest
// known key) when the miss is close to a real one. Runs as a warning-level
// heuristic: diagnostics surface at blueprint-validation time rather than
// at edge-evaluation time deep inside a run.
func validateEdgeReferences(bp dag.Blueprint, issues *[]BlueprintValidationIssue) {
	nodeTypes := make(map[string]string, len(bp.Nodes))
	for _, node := range bp.Nodes {
		nodeTypes[node.ID] = node.Type
	}
	for _, edge := range bp.Edges {
		cond := strings.TrimSpace(edge.Condition)
		if cond == "" {
			continue
		}
		edgeID := edge.From + "->" + edge.To
		for _, ident := range dag.ExtractIdentifiers(cond) {
			// Only `results.<node>.<field>` references can flag an unknown
			// output key. Other namespaces (`inputs.`, `run.`) and bare
			// identifiers don't address node outputs.
			if !strings.HasPrefix(ident, "results.") {
				continue
			}
			rest := strings.TrimPrefix(ident, "results.")
			dot := strings.IndexByte(rest, '.')
			if dot <= 0 {
				continue
			}
			nodeID := rest[:dot]
			key := rest[dot+1:]
			nodeType, ok := nodeTypes[nodeID]
			if !ok {
				continue
			}
			keys := canonicalOutputKeys(nodeType)
			if len(keys) == 0 {
				continue // executor didn't declare output keys, skip
			}
			if key == "status" || key == "history" {
				continue // status is universal; history is the back-edge accessor
			}
			if indexOfString(keys, key) >= 0 {
				continue
			}
			// Unknown key — check if it's a prefix match on a declared key
			// (e.g. "fetch.raw" when the real key is "fetch.raw.body"):
			// agents frequently reference nested paths under a declared
			// string output, which CEL handles fine. Skip.
			if hasKeyPrefix(keys, key) {
				continue
			}
			msg := fmt.Sprintf("Edge %q references %q but %q does not declare that output key.", edgeID, ident, nodeType)
			if suggestion := closestKey(key, keys); suggestion != "" {
				msg += fmt.Sprintf(" Did you mean %q?", "results."+nodeID+"."+suggestion)
			}
			*issues = append(*issues, BlueprintValidationIssue{
				Code:     "EDGE_UNKNOWN_OUTPUT_KEY",
				Message:  msg,
				Target:   edgeID,
				Severity: "warning",
			})
		}
	}
}

// canonicalOutputKeys returns the output keys declared by the production
// executor for nodeType. Duplicates the runner's registration list, which is
// intentional: this validator does not hold a live engine, and the runner's
// engine isn't import-safe from this package.
func canonicalOutputKeys(nodeType string) []string {
	switch nodeType {
	case "api_fetch":
		return (&executors.APIFetchExecutor{}).OutputKeys()
	case "llm_call":
		return (&executors.LLMCallExecutor{}).OutputKeys()
	case "agent_loop":
		return (&executors.AgentLoopExecutor{}).OutputKeys()
	case "return":
		return (&executors.ReturnExecutor{}).OutputKeys()
	case "await_signal":
		return (&executors.AwaitSignalExecutor{}).OutputKeys()
	case "wait":
		return (&executors.WaitExecutor{}).OutputKeys()
	case "cel_eval":
		// cel_eval keys come from the user's expressions map, not from a
		// static declaration. Skip the diagnostic for these.
		return nil
	case "map":
		return (&executors.MapExecutor{}).OutputKeys()
	case "gadget":
		return (&executors.GadgetExecutor{}).OutputKeys()
	case "validate_blueprint":
		return (&executors.ValidateBlueprintExecutor{}).OutputKeys()
	}
	return nil
}

func indexOfString(values []string, target string) int {
	for i, v := range values {
		if v == target {
			return i
		}
	}
	return -1
}

func hasKeyPrefix(keys []string, candidate string) bool {
	for _, k := range keys {
		if candidate == k {
			return true
		}
		if strings.HasPrefix(candidate, k+".") {
			return true
		}
	}
	return false
}

// closestKey returns the declared key with the smallest edit distance to
// candidate, or "" when no key is within 3 edits.
func closestKey(candidate string, keys []string) string {
	best := ""
	bestDist := 4
	for _, k := range keys {
		d := levenshtein(candidate, k)
		if d < bestDist {
			best = k
			bestDist = d
		}
	}
	return best
}

func levenshtein(a, b string) int {
	if a == b {
		return 0
	}
	la, lb := len(a), len(b)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}
	prev := make([]int, lb+1)
	curr := make([]int, lb+1)
	for j := 0; j <= lb; j++ {
		prev[j] = j
	}
	for i := 1; i <= la; i++ {
		curr[0] = i
		for j := 1; j <= lb; j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			del := prev[j] + 1
			ins := curr[j-1] + 1
			sub := prev[j-1] + cost
			m := del
			if ins < m {
				m = ins
			}
			if sub < m {
				m = sub
			}
			curr[j] = m
		}
		prev, curr = curr, prev
	}
	return prev[lb]
}

func validateNodeConfig(node dag.NodeDef, issues *[]BlueprintValidationIssue) {
	add := func(code, message string) {
		*issues = append(*issues, BlueprintValidationIssue{Code: code, Message: message, Target: node.ID, Severity: "error"})
	}
	switch node.Type {
	case "api_fetch":
		cfg, err := executors.ParseConfig[executors.APIFetchConfig](node.Config)
		if err != nil {
			add("API_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", node.DisplayName()))
			return
		}
		if strings.TrimSpace(cfg.URL) == "" {
			add("API_URL_REQUIRED", fmt.Sprintf("Node %q needs a URL.", node.DisplayName()))
		}
		if strings.TrimSpace(cfg.URL) != "" {
			if _, err := url.ParseRequestURI(cfg.URL); err != nil || !strings.Contains(cfg.URL, "://") {
				add("API_URL_INVALID", fmt.Sprintf("Node %q needs a valid absolute URL.", node.DisplayName()))
			}
		}
		if strings.TrimSpace(cfg.Method) != "" {
			if _, err := executors.NormalizeAPIFetchMethod(cfg.Method); err != nil {
				add("API_METHOD_INVALID", fmt.Sprintf("Node %q uses an unsupported HTTP method.", node.DisplayName()))
			}
		}
		if cfg.TimeoutSeconds < 0 {
			add("API_TIMEOUT_INVALID", fmt.Sprintf("Node %q needs a non-negative timeout.", node.DisplayName()))
		}
	case "llm_call":
		cfg, err := executors.ParseConfig[executors.LLMCallConfig](node.Config)
		if err != nil {
			add("LLM_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", node.DisplayName()))
			return
		}
		if strings.TrimSpace(cfg.Prompt) == "" {
			add("LLM_PROMPT_REQUIRED", fmt.Sprintf("Node %q needs a prompt.", node.DisplayName()))
		}
		if cfg.AllowedOutcomesKey != "" && strings.TrimSpace(cfg.AllowedOutcomesKey) == "" {
			add("LLM_ALLOWED_OUTCOMES_KEY_INVALID", fmt.Sprintf("Node %q has an empty allowed outcomes key.", node.DisplayName()))
		}
	case "agent_loop":
		cfg, err := executors.ParseConfig[executors.AgentLoopConfig](node.Config)
		if err != nil {
			add("AGENT_LOOP_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", node.DisplayName()))
			return
		}
		if strings.TrimSpace(cfg.Prompt) == "" {
			add("AGENT_LOOP_PROMPT_REQUIRED", fmt.Sprintf("Node %q needs a prompt.", node.DisplayName()))
		}
		outputMode := strings.ToLower(strings.TrimSpace(cfg.OutputMode))
		if outputMode == "" {
			outputMode = executors.AgentOutputModeText
		}
		switch outputMode {
		case executors.AgentOutputModeText, executors.AgentOutputModeStructured, executors.AgentOutputModeResolution:
		default:
			add("AGENT_LOOP_OUTPUT_MODE_INVALID", fmt.Sprintf("Node %q has an unsupported output_mode.", node.DisplayName()))
		}
		if outputMode == executors.AgentOutputModeStructured {
			if cfg.OutputTool == nil || len(cfg.OutputTool.Parameters) == 0 {
				add("AGENT_LOOP_OUTPUT_TOOL_REQUIRED", fmt.Sprintf("Node %q structured output needs output_tool.parameters.", node.DisplayName()))
			}
		}
		if cfg.OutputTool != nil && len(cfg.OutputTool.Parameters) > 0 && !json.Valid(cfg.OutputTool.Parameters) {
			add("AGENT_LOOP_OUTPUT_TOOL_INVALID", fmt.Sprintf("Node %q output_tool.parameters must be valid JSON.", node.DisplayName()))
		}
		if cfg.TimeoutSeconds < 0 || cfg.ToolTimeoutSeconds < 0 {
			add("AGENT_LOOP_TIMEOUT_INVALID", fmt.Sprintf("Node %q needs non-negative timeouts.", node.DisplayName()))
		}
		if cfg.MaxSteps < 0 || cfg.MaxToolCalls < 0 || cfg.MaxToolResultBytes < 0 || cfg.MaxTokens < 0 {
			add("AGENT_LOOP_LIMIT_INVALID", fmt.Sprintf("Node %q needs non-negative limits.", node.DisplayName()))
		}
		if cfg.AllowedOutcomesKey != "" && strings.TrimSpace(cfg.AllowedOutcomesKey) == "" {
			add("AGENT_LOOP_ALLOWED_OUTCOMES_KEY_INVALID", fmt.Sprintf("Node %q has an empty allowed outcomes key.", node.DisplayName()))
		}
		validateAgentLoopTools(node, cfg, add)
	case "await_signal":
		cfg, err := executors.ParseConfig[executors.AwaitSignalConfig](node.Config)
		if err != nil {
			add("AWAIT_SIGNAL_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", node.DisplayName()))
			return
		}
		if strings.TrimSpace(cfg.SignalType) == "" {
			add("AWAIT_SIGNAL_TYPE_REQUIRED", fmt.Sprintf("Node %q needs a signal_type.", node.DisplayName()))
		}
		if cfg.TimeoutSeconds < 0 || cfg.TimeoutSeconds > 604800 {
			add("AWAIT_SIGNAL_TIMEOUT_INVALID", fmt.Sprintf("Node %q needs a timeout between 0 and 604800 seconds.", node.DisplayName()))
		}
		for _, key := range cfg.RequiredPayload {
			if strings.TrimSpace(key) == "" {
				add("AWAIT_SIGNAL_REQUIRED_PAYLOAD_INVALID", fmt.Sprintf("Node %q has an empty required payload key.", node.DisplayName()))
			}
		}
	case "wait":
		cfg, err := executors.ParseConfig[executors.WaitConfig](node.Config)
		if err != nil {
			add("WAIT_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", node.DisplayName()))
			return
		}
		if cfg.DurationSeconds < 0 {
			add("WAIT_DURATION_INVALID", fmt.Sprintf("Node %q needs a non-negative wait duration.", node.DisplayName()))
		}
		if cfg.Mode != "" && cfg.Mode != "sleep" && cfg.Mode != "defer" {
			add("WAIT_MODE_INVALID", fmt.Sprintf("Node %q must use either sleep or defer mode.", node.DisplayName()))
		}
		if cfg.StartFrom != "" && cfg.StartFrom != "now" && cfg.StartFrom != "deadline" && cfg.StartFrom != "resolution_pending_since" {
			add("WAIT_START_FROM_INVALID", fmt.Sprintf("Node %q has an unsupported wait anchor.", node.DisplayName()))
		}
		if cfg.Mode == "defer" && cfg.StartFrom == "now" {
			add("WAIT_DEFER_START_NOW_UNSUPPORTED", fmt.Sprintf("Node %q cannot use \"now\" with defer mode.", node.DisplayName()))
		}
		if cfg.MaxInlineSeconds > executors.MaxInlineWaitSecondsCap {
			add("WAIT_MAX_INLINE_TOO_LARGE", fmt.Sprintf("Node %q max_inline_seconds must be <= %d (inline waits pin an executor goroutine).", node.DisplayName(), executors.MaxInlineWaitSecondsCap))
		}
	case "return":
		cfg, err := executors.ParseConfig[executors.ReturnConfig](node.Config)
		if err != nil {
			add("RETURN_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", node.DisplayName()))
			return
		}
		if err := executors.ValidateReturnConfig(cfg); err != nil {
			add("RETURN_CONFIG_INVALID", fmt.Sprintf("Node %q: %s", node.DisplayName(), err.Error()))
		}
	case "cel_eval":
		cfg, err := executors.ParseConfig[executors.CelEvalConfig](node.Config)
		if err != nil {
			add("CEL_EVAL_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", node.DisplayName()))
			return
		}
		if len(cfg.Expressions) == 0 {
			add("CEL_EVAL_EXPRESSIONS_REQUIRED", fmt.Sprintf("Node %q needs at least one expression.", node.DisplayName()))
		}
	case "map":
		cfg, err := executors.ParseConfig[executors.MapConfig](node.Config)
		if err != nil {
			add("MAP_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", node.DisplayName()))
			return
		}
		if strings.TrimSpace(cfg.ItemsKey) == "" {
			add("MAP_ITEMS_KEY_REQUIRED", fmt.Sprintf("Node %q needs an items_key.", node.DisplayName()))
		}
		if cfg.Inline == nil || len(cfg.Inline.Nodes) == 0 {
			add("MAP_INLINE_REQUIRED", fmt.Sprintf("Node %q needs an inline blueprint.", node.DisplayName()))
		}
		if cfg.Inline != nil {
			if errs := dag.ValidateBlueprint(*cfg.Inline); len(errs) > 0 {
				add("MAP_INLINE_INVALID", fmt.Sprintf("Node %q inline blueprint: %v", node.DisplayName(), errs[0]))
			} else if err := executors.FirstReturnContractError(*cfg.Inline); err != nil {
				add("MAP_INLINE_INVALID", fmt.Sprintf("Node %q inline blueprint: %v", node.DisplayName(), err))
			}
		}
		if cfg.BatchSize < 0 {
			add("MAP_BATCH_SIZE_INVALID", fmt.Sprintf("Node %q batch_size must be non-negative.", node.DisplayName()))
		}
		if cfg.MaxConcurrency != nil && *cfg.MaxConcurrency < 0 {
			add("MAP_MAX_CONCURRENCY_INVALID", fmt.Sprintf("Node %q max_concurrency must be non-negative.", node.DisplayName()))
		}
		if cfg.MaxItems < 0 {
			add("MAP_MAX_ITEMS_INVALID", fmt.Sprintf("Node %q max_items must be non-negative.", node.DisplayName()))
		}
		if cfg.MaxDepth < 0 {
			add("MAP_MAX_DEPTH_INVALID", fmt.Sprintf("Node %q max_depth must be non-negative.", node.DisplayName()))
		}
		if cfg.PerBatchTimeoutSeconds < 0 {
			add("MAP_TIMEOUT_INVALID", fmt.Sprintf("Node %q per_batch_timeout_seconds must be non-negative.", node.DisplayName()))
		}
		onError := strings.TrimSpace(cfg.OnError)
		if onError != "" && onError != "fail" && onError != "continue" {
			add("MAP_ON_ERROR_INVALID", fmt.Sprintf("Node %q on_error must be \"fail\" or \"continue\".", node.DisplayName()))
		}
	case "gadget":
		cfg, err := executors.ParseConfig[executors.GadgetConfig](node.Config)
		if err != nil {
			add("GADGET_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", node.DisplayName()))
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
			add("GADGET_BLUEPRINT_SOURCE_REQUIRED", fmt.Sprintf("Node %q needs exactly one of blueprint_json, blueprint_json_key, or inline.", node.DisplayName()))
		case sourceCount > 1:
			add("GADGET_BLUEPRINT_SOURCE_CONFLICT", fmt.Sprintf("Node %q can use only one of blueprint_json, blueprint_json_key, or inline.", node.DisplayName()))
		}
		if cfg.TimeoutSeconds < 0 {
			add("GADGET_TIMEOUT_INVALID", fmt.Sprintf("Node %q timeout_seconds must be non-negative.", node.DisplayName()))
		}
		if cfg.MaxDepth < 0 {
			add("GADGET_MAX_DEPTH_INVALID", fmt.Sprintf("Node %q max_depth must be non-negative.", node.DisplayName()))
		}
		if policy := cfg.DynamicBlueprintPolicy; policy != nil {
			if policy.MaxNodes < 0 ||
				policy.MaxEdges < 0 ||
				policy.MaxDepth < 0 ||
				policy.MaxTotalTimeSeconds < 0 ||
				policy.MaxTotalTokens < 0 {
				add("GADGET_DYNAMIC_POLICY_INVALID", fmt.Sprintf("Node %q dynamic blueprint policy limits must be non-negative.", node.DisplayName()))
			}
		}
		if cfg.Inline != nil {
			rawInline, err := json.Marshal(cfg.Inline)
			if err != nil {
				add("GADGET_INLINE_INVALID", fmt.Sprintf("Node %q inline blueprint could not be serialized.", node.DisplayName()))
			} else if child := ValidateResolutionBlueprint(*cfg.Inline, rawInline); !child.Valid {
				first := child.Issues[0]
				add("GADGET_INLINE_INVALID", fmt.Sprintf("Node %q inline blueprint is invalid: %s", node.DisplayName(), strings.TrimSpace(first.Message)))
			}
		}
		if raw := strings.TrimSpace(cfg.BlueprintJSON); raw != "" && !strings.Contains(raw, "{{") {
			if !json.Valid([]byte(raw)) {
				add("GADGET_BLUEPRINT_JSON_INVALID", fmt.Sprintf("Node %q blueprint_json must be valid JSON text.", node.DisplayName()))
			} else {
				var child dag.Blueprint
				if err := json.Unmarshal([]byte(raw), &child); err != nil {
					add("GADGET_BLUEPRINT_JSON_INVALID", fmt.Sprintf("Node %q blueprint_json could not be parsed.", node.DisplayName()))
				} else if result := ValidateResolutionBlueprint(child, []byte(raw)); !result.Valid {
					first := result.Issues[0]
					add("GADGET_BLUEPRINT_INVALID", fmt.Sprintf("Node %q blueprint_json is invalid: %s", node.DisplayName(), strings.TrimSpace(first.Message)))
				}
			}
		}
	case "validate_blueprint":
		cfg, err := executors.ParseConfig[executors.ValidateBlueprintConfig](node.Config)
		if err != nil {
			add("VALIDATE_BLUEPRINT_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", node.DisplayName()))
			return
		}
		if strings.TrimSpace(cfg.BlueprintJSONKey) == "" {
			add("VALIDATE_BLUEPRINT_KEY_REQUIRED", fmt.Sprintf("Node %q needs a blueprint_json_key.", node.DisplayName()))
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
			add("AGENT_LOOP_TOOL_NAME_REQUIRED", fmt.Sprintf("Node %q has an agent tool without a name.", node.DisplayName()))
		}
		if len(tool.Parameters) > 0 && !json.Valid(tool.Parameters) {
			add("AGENT_LOOP_TOOL_PARAMETERS_INVALID", fmt.Sprintf("Node %q tool %q parameters must be valid JSON.", node.DisplayName(), name))
		}
		if tool.TimeoutSeconds < 0 {
			add("AGENT_LOOP_TOOL_TIMEOUT_INVALID", fmt.Sprintf("Node %q tool %q needs a non-negative timeout.", node.DisplayName(), name))
		}
		if tool.MaxDepth < 0 {
			add("AGENT_LOOP_TOOL_MAX_DEPTH_INVALID", fmt.Sprintf("Node %q tool %q needs a non-negative max_depth.", node.DisplayName(), name))
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
				add("AGENT_LOOP_BUILTIN_TOOL_INVALID", fmt.Sprintf("Node %q uses unknown builtin tool %q.", node.DisplayName(), builtin))
			}
			if builtin == executors.AgentBuiltinRunBlueprint && !cfg.EnableDynamicBlueprint {
				add("AGENT_LOOP_DYNAMIC_BLUEPRINT_DISABLED", fmt.Sprintf("Node %q tool %q requires enable_dynamic_blueprints.", node.DisplayName(), name))
			}
		case executors.AgentToolKindBlueprint:
			if tool.Inline == nil || len(tool.Inline.Nodes) == 0 {
				add("AGENT_LOOP_BLUEPRINT_TOOL_INLINE_REQUIRED", fmt.Sprintf("Node %q tool %q needs an inline blueprint.", node.DisplayName(), name))
				continue
			}
			if errs := dag.ValidateBlueprint(*tool.Inline); len(errs) > 0 {
				add("AGENT_LOOP_BLUEPRINT_TOOL_INLINE_INVALID", fmt.Sprintf("Node %q tool %q inline blueprint: %v.", node.DisplayName(), name, errs[0]))
			} else if err := executors.FirstReturnContractError(*tool.Inline); err != nil {
				add("AGENT_LOOP_BLUEPRINT_TOOL_INLINE_INVALID", fmt.Sprintf("Node %q tool %q inline blueprint: %v.", node.DisplayName(), name, err))
			}
		default:
			add("AGENT_LOOP_TOOL_KIND_INVALID", fmt.Sprintf("Node %q tool %q has unsupported kind %q.", node.DisplayName(), name, tool.Kind))
		}
	}

	if policy := cfg.DynamicBlueprintPolicy; policy != nil {
		if policy.MaxNodes < 0 ||
			policy.MaxEdges < 0 ||
			policy.MaxDepth < 0 ||
			policy.MaxTotalTimeSeconds < 0 ||
			policy.MaxTotalTokens < 0 {
			add("AGENT_LOOP_DYNAMIC_POLICY_INVALID", fmt.Sprintf("Node %q dynamic blueprint policy limits must be non-negative.", node.DisplayName()))
		}
	}
}
