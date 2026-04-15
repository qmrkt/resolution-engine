package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/question-market/resolution-engine/dag"
	"github.com/question-market/resolution-engine/executors"
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
	"api_fetch":           {},
	"market_evidence":     {},
	"llm_judge":           {},
	"human_judge":         {},
	"wait":                {},
	"defer_resolution":    {},
	"submit_result":       {},
	"cancel_market":       {},
	"ask_creator":         {},
	"ask_market_admin":    {},
	"outcome_terminality": {},
}

func ValidateResolutionBlueprint(bp dag.Blueprint, rawJSON []byte) BlueprintValidationResult {
	issues := make([]BlueprintValidationIssue, 0)
	add := func(code, message, target string) {
		issues = append(issues, BlueprintValidationIssue{Code: code, Message: message, Target: target, Severity: "error"})
	}

	if len(bp.Nodes) == 0 {
		add("EMPTY_BLUEPRINT", "Add at least one node to the blueprint.", "")
	}
	if len(bp.Nodes) > 16 {
		add("TOO_MANY_NODES", "Blueprints are capped at 16 nodes in V1.", "")
	}
	if len(rawJSON) > 8192 {
		add("BLUEPRINT_TOO_LARGE", fmt.Sprintf("Resolution blueprint exceeds 8KB limit: %d bytes.", len(rawJSON)), "")
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
		cfg, err := parseConfigForValidation[executors.APIFetchConfig](node.Config)
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
		if strings.TrimSpace(cfg.JSONPath) == "" {
			add("API_JSON_PATH_REQUIRED", fmt.Sprintf("Node %q needs a JSON path.", displayNode(node)))
		}
	case "llm_judge":
		cfg, err := parseConfigForValidation[executors.LLMJudgeConfig](node.Config)
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
	case "human_judge":
		cfg, err := parseConfigForValidation[executors.HumanJudgeConfig](node.Config)
		if err != nil {
			add("HUMAN_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", displayNode(node)))
			return
		}
		if strings.TrimSpace(cfg.Prompt) == "" {
			add("HUMAN_PROMPT_REQUIRED", fmt.Sprintf("Node %q needs a prompt.", displayNode(node)))
		}
		if len(cfg.AllowedResponders) == 0 {
			add("HUMAN_RESPONDERS_REQUIRED", fmt.Sprintf("Node %q needs at least one allowed responder.", displayNode(node)))
		}
		if cfg.TimeoutSeconds < 300 || cfg.TimeoutSeconds > 604800 {
			add("HUMAN_TIMEOUT_INVALID", fmt.Sprintf("Node %q needs a timeout between 300 and 604800 seconds.", displayNode(node)))
		}
		hasDesignated := false
		for _, responder := range cfg.AllowedResponders {
			if responder == "designated" {
				hasDesignated = true
				break
			}
		}
		if hasDesignated && strings.TrimSpace(cfg.DesignatedAddress) == "" {
			add("HUMAN_DESIGNATED_ADDRESS_REQUIRED", fmt.Sprintf("Node %q has a designated responder but no address.", displayNode(node)))
		}
	case "wait":
		cfg, err := parseConfigForValidation[executors.WaitConfig](node.Config)
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
		cfg, err := parseConfigForValidation[executors.SubmitResultConfig](node.Config)
		if err != nil {
			add("SUBMIT_CONFIG_INVALID", fmt.Sprintf("Node %q has invalid config.", displayNode(node)))
			return
		}
		if strings.TrimSpace(cfg.OutcomeKey) == "" {
			add("SUBMIT_OUTCOME_KEY_REQUIRED", fmt.Sprintf("Node %q needs an outcome source.", displayNode(node)))
		}
	}
}

func parseConfigForValidation[T any](raw interface{}) (T, error) {
	var cfg T
	data, err := json.Marshal(raw)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}
