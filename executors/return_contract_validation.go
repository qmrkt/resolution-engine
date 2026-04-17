package executors

import (
	"fmt"
	"strings"

	"github.com/qmrkt/resolution-engine/dag"
)

func ValidateReturnContract(bp dag.Blueprint) []BlueprintValidationIssue {
	issues := make([]BlueprintValidationIssue, 0)
	add := func(code, message, target string) {
		issues = append(issues, BlueprintValidationIssue{
			Code:     code,
			Message:  message,
			Target:   target,
			Severity: "error",
		})
	}

	if len(bp.Nodes) == 0 {
		add("NO_RETURN_NODE", "Blueprint needs at least one return node.", "")
		return issues
	}

	scheduler := dag.NewScheduler(bp)
	forwards := collectForwardEdges(bp, scheduler)

	roots := rootsFromForwardEdges(bp, forwards)
	if len(roots) == 0 {
		add("NO_ROOT", "Blueprint needs at least one root node.", "")
	}

	reachable := reachableViaForwardEdges(bp, forwards, roots)
	returnNodes := make(map[string]dag.NodeDef)
	outgoingByNode := make(map[string][]dag.EdgeDef, len(bp.Nodes))
	for _, node := range bp.Nodes {
		outgoingByNode[node.ID] = nil
		if node.Type == "return" {
			returnNodes[node.ID] = node
		}
	}
	// Use the full edge set: a node whose only outgoing edge is a back-edge
	// is not a leaf — the loop iteration may eventually reach a return on a
	// later pass.
	for _, edge := range bp.Edges {
		outgoingByNode[edge.From] = append(outgoingByNode[edge.From], edge)
	}

	if len(returnNodes) == 0 {
		add("NO_RETURN_NODE", "Blueprint needs at least one return node.", "")
	}

	for _, node := range bp.Nodes {
		if len(roots) > 0 {
			if _, ok := reachable[node.ID]; !ok {
				add("UNREACHABLE_NODE", fmt.Sprintf("Node %q is unreachable from the graph roots.", node.DisplayName()), node.ID)
			}
		}
		outgoing := outgoingByNode[node.ID]
		if node.Type == "return" {
			if len(outgoing) > 0 {
				add("RETURN_HAS_OUTGOING", fmt.Sprintf("Return node %q cannot have outgoing edges.", node.DisplayName()), node.ID)
			}
			continue
		}
		if _, ok := reachable[node.ID]; ok && len(outgoing) == 0 {
			add("NON_RETURN_LEAF", fmt.Sprintf("Node %q needs an outgoing path to a return node.", node.DisplayName()), node.ID)
		}
	}

	canReachReturn := reachabilityToReturn(bp)
	for _, node := range bp.Nodes {
		if _, ok := reachable[node.ID]; !ok {
			continue
		}
		if _, ok := canReachReturn[node.ID]; !ok {
			add("NO_RETURN_PATH", fmt.Sprintf("Node %q does not lead to any return node.", node.DisplayName()), node.ID)
		}
	}

	return issues
}

func FirstReturnContractError(bp dag.Blueprint) error {
	issues := ValidateReturnContract(bp)
	if len(issues) == 0 {
		return nil
	}
	return fmt.Errorf("%s", strings.TrimSpace(issues[0].Message))
}

// collectForwardEdges returns the blueprint's edges minus the back-edges
// that close loops. Back-edges don't make a target "reachable from above"
// or "able to reach a return below" — the loop already had to be entered
// through a forward path, and the return must be on that forward path.
func collectForwardEdges(bp dag.Blueprint, scheduler *dag.Scheduler) []dag.EdgeDef {
	out := make([]dag.EdgeDef, 0, len(bp.Edges))
	for _, edge := range bp.Edges {
		if scheduler.IsBackEdge(edge.From, edge.To) {
			continue
		}
		out = append(out, edge)
	}
	return out
}

func rootsFromForwardEdges(bp dag.Blueprint, forwards []dag.EdgeDef) []string {
	hasIncoming := make(map[string]struct{}, len(forwards))
	for _, edge := range forwards {
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

func reachableViaForwardEdges(bp dag.Blueprint, forwards []dag.EdgeDef, roots []string) map[string]struct{} {
	adjacency := make(map[string][]string, len(bp.Nodes))
	for _, node := range bp.Nodes {
		adjacency[node.ID] = nil
	}
	for _, edge := range forwards {
		adjacency[edge.From] = append(adjacency[edge.From], edge.To)
	}
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
	return reachable
}

// reachabilityToReturn does a reverse BFS from every return node. Back-edges
// are included: a node whose only outgoing path is a back-edge can still
// reach a return via subsequent loop iterations (subject to MaxTraversals),
// so excluding them would flag legitimate retry/repair loops as dead-ends.
func reachabilityToReturn(bp dag.Blueprint) map[string]struct{} {
	reverse := make(map[string][]string, len(bp.Nodes))
	for _, node := range bp.Nodes {
		reverse[node.ID] = nil
	}
	queue := make([]string, 0)
	for _, edge := range bp.Edges {
		reverse[edge.To] = append(reverse[edge.To], edge.From)
	}
	for _, node := range bp.Nodes {
		if node.Type == "return" {
			queue = append(queue, node.ID)
		}
	}
	reachable := make(map[string]struct{}, len(bp.Nodes))
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

