package executors

import (
	"fmt"
	"strings"

	"github.com/qmrkt/resolution-engine/dag"
)

func validateRuntimeBlueprintStructure(bp dag.Blueprint, policy DynamicBlueprintPolicy) error {
	if policy.MaxNodes > 0 && len(bp.Nodes) > policy.MaxNodes {
		return fmt.Errorf("node count %d exceeds max_nodes %d", len(bp.Nodes), policy.MaxNodes)
	}
	if policy.MaxEdges > 0 && len(bp.Edges) > policy.MaxEdges {
		return fmt.Errorf("edge count %d exceeds max_edges %d", len(bp.Edges), policy.MaxEdges)
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
		if len(allowed) > 0 {
			if _, ok := allowed[node.Type]; !ok {
				return fmt.Errorf("node type %q is not allowed", node.Type)
			}
		}
	}

	budget := dag.Budget{}
	if bp.Budget != nil {
		budget = *bp.Budget
	}
	if policy.MaxTotalTimeSeconds > 0 &&
		(budget.MaxTotalTimeSeconds <= 0 || budget.MaxTotalTimeSeconds > policy.MaxTotalTimeSeconds) {
		return fmt.Errorf("blueprint budget must set max_total_time_seconds <= %d", policy.MaxTotalTimeSeconds)
	}
	if policy.MaxTotalTokens > 0 &&
		(budget.MaxTotalTokens <= 0 || budget.MaxTotalTokens > policy.MaxTotalTokens) {
		return fmt.Errorf("blueprint budget must set max_total_tokens <= %d", policy.MaxTotalTokens)
	}

	if errs := dag.ValidateBlueprint(bp); len(errs) > 0 {
		return errs[0]
	}
	return nil
}
