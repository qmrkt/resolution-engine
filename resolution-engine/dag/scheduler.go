package dag

// Scheduler computes topology and evaluates DAG traversal rules.

import "strings"

const (
	colorWhite = 0
	colorGray  = 1
	colorBlack = 2
)

// Scheduler analyzes the blueprint graph and drives execution ordering.
type Scheduler struct {
	blueprint      Blueprint
	incoming       map[string][]EdgeDef
	outgoing       map[string][]EdgeDef
	backEdges      map[string]struct{}
	topology       []string
	topologyIndex  map[string]int
	Skipped        map[string]struct{}
	edgeTraversals map[string]int
}

// NewScheduler precomputes scheduling indexes for a blueprint.
func NewScheduler(blueprint Blueprint) *Scheduler {
	s := &Scheduler{
		blueprint:      blueprint,
		incoming:       make(map[string][]EdgeDef, len(blueprint.Nodes)),
		outgoing:       make(map[string][]EdgeDef, len(blueprint.Nodes)),
		backEdges:      make(map[string]struct{}),
		topologyIndex:  make(map[string]int, len(blueprint.Nodes)),
		Skipped:        make(map[string]struct{}),
		edgeTraversals: make(map[string]int),
	}

	nodeIDs := make(map[string]struct{}, len(blueprint.Nodes))
	for _, node := range blueprint.Nodes {
		nodeIDs[node.ID] = struct{}{}
		s.incoming[node.ID] = nil
		s.outgoing[node.ID] = nil
	}

	for _, edge := range blueprint.Edges {
		if _, ok := nodeIDs[edge.From]; !ok {
			continue
		}
		if _, ok := nodeIDs[edge.To]; !ok {
			continue
		}
		s.outgoing[edge.From] = append(s.outgoing[edge.From], edge)
		s.incoming[edge.To] = append(s.incoming[edge.To], edge)
	}

	s.backEdges = detectBackEdges(blueprint.Nodes, s.outgoing)
	s.topology = computeTopology(blueprint.Nodes, s.outgoing, s.backEdges)
	for idx, nodeID := range s.topology {
		s.topologyIndex[nodeID] = idx
	}

	return s
}

// Topology returns the topological order of forward edges.
func (s *Scheduler) Topology() []string {
	out := make([]string, len(s.topology))
	copy(out, s.topology)
	return out
}

// IsBackEdge reports whether an edge is a cycle-closing back-edge.
func (s *Scheduler) IsBackEdge(from, to string) bool {
	_, ok := s.backEdges[edgeKey(from, to)]
	return ok
}

// IsSkipped reports whether the scheduler has marked a node as skipped.
func (s *Scheduler) IsSkipped(nodeID string) bool {
	if s == nil {
		return false
	}
	_, ok := s.Skipped[nodeID]
	return ok
}

// TraversalSnapshot returns a copy of the current edge traversal counts.
func (s *Scheduler) TraversalSnapshot() map[string]int {
	if s == nil {
		return nil
	}
	snap := make(map[string]int, len(s.edgeTraversals))
	for key, value := range s.edgeTraversals {
		snap[key] = value
	}
	return snap
}

// ReadyNodes returns nodes that can execute now.
func (s *Scheduler) ReadyNodes(completed, failed, running map[string]struct{}) []string {
	ready := make([]string, 0)
	for _, nodeID := range s.topology {
		if inSet(completed, nodeID) || inSet(failed, nodeID) || inSet(running, nodeID) {
			continue
		}
		if _, skipped := s.Skipped[nodeID]; skipped {
			continue
		}

		incoming := s.incoming[nodeID]
		if len(incoming) == 0 {
			ready = append(ready, nodeID)
			continue
		}

		// Only forward edges block readiness; back-edges don't
		allCompleted := true
		hasFailedUpstream := false
		forwardCount := 0
		for _, edge := range incoming {
			if s.IsBackEdge(edge.From, edge.To) {
				continue
			}
			forwardCount++
			src := edge.From
			if inSet(failed, src) || inSet(s.Skipped, src) {
				hasFailedUpstream = true
			}
			if !inSet(completed, src) {
				allCompleted = false
			}
		}

		if forwardCount == 0 {
			ready = append(ready, nodeID)
			continue
		}

		if len(incoming) > 1 && hasFailedUpstream {
			s.Skipped[nodeID] = struct{}{}
			continue
		}

		if allCompleted {
			ready = append(ready, nodeID)
		}
	}

	return ready
}

// EvaluateEdges returns outgoing edges whose conditions pass.
func (s *Scheduler) EvaluateEdges(nodeID string, ctx *Context) []EdgeDef {
	outgoing := s.outgoing[nodeID]
	if len(outgoing) == 0 {
		return nil
	}

	type backCandidate struct {
		edge      EdgeDef
		exhausted bool
	}

	backCandidates := make([]backCandidate, 0)
	forwardCandidates := make([]EdgeDef, 0)

	for _, edge := range outgoing {
		if !s.edgeConditionMet(edge, ctx) {
			continue
		}

		if s.IsBackEdge(edge.From, edge.To) {
			backCandidates = append(backCandidates, backCandidate{
				edge:      edge,
				exhausted: s.edgeTraversalExhausted(edge),
			})
			continue
		}

		if s.edgeTraversalExhausted(edge) {
			continue
		}
		forwardCandidates = append(forwardCandidates, edge)
	}

	if len(backCandidates) > 0 {
		activeBack := make([]EdgeDef, 0, len(backCandidates))
		allExhausted := true
		for _, c := range backCandidates {
			if !c.exhausted {
				allExhausted = false
				activeBack = append(activeBack, c.edge)
			}
		}

		if len(activeBack) > 0 {
			return activeBack
		}
		if allExhausted {
			return forwardCandidates
		}
	}

	return forwardCandidates
}

// TrackTraversal increments the traversal counter for an edge.
func (s *Scheduler) TrackTraversal(from, to string) {
	s.edgeTraversals[edgeKey(from, to)]++
}

func (s *Scheduler) edgeConditionMet(edge EdgeDef, ctx *Context) bool {
	if strings.TrimSpace(edge.Condition) == "" {
		return true
	}
	ok, err := EvalCondition(edge.Condition, ctx)
	if err != nil {
		return false
	}
	return ok
}

func (s *Scheduler) edgeTraversalExhausted(edge EdgeDef) bool {
	if s.IsBackEdge(edge.From, edge.To) && edge.MaxTraversals <= 0 {
		return true
	}
	if edge.MaxTraversals <= 0 {
		return false
	}
	return s.edgeTraversals[edgeKey(edge.From, edge.To)] >= edge.MaxTraversals
}

func computeTopology(nodes []NodeDef, outgoing map[string][]EdgeDef, backEdges map[string]struct{}) []string {
	indegree := make(map[string]int, len(nodes))
	for _, node := range nodes {
		indegree[node.ID] = 0
	}

	for _, node := range nodes {
		for _, edge := range outgoing[node.ID] {
			if _, isBack := backEdges[edgeKey(edge.From, edge.To)]; isBack {
				continue
			}
			if _, ok := indegree[edge.To]; ok {
				indegree[edge.To]++
			}
		}
	}

	queue := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if indegree[node.ID] == 0 {
			queue = append(queue, node.ID)
		}
	}

	order := make([]string, 0, len(nodes))
	seen := make(map[string]struct{}, len(nodes))

	for i := 0; i < len(queue); i++ {
		nodeID := queue[i]
		if _, already := seen[nodeID]; already {
			continue
		}
		seen[nodeID] = struct{}{}
		order = append(order, nodeID)

		for _, edge := range outgoing[nodeID] {
			if _, isBack := backEdges[edgeKey(edge.From, edge.To)]; isBack {
				continue
			}
			if _, ok := indegree[edge.To]; !ok {
				continue
			}
			indegree[edge.To]--
			if indegree[edge.To] == 0 {
				queue = append(queue, edge.To)
			}
		}
	}

	for _, node := range nodes {
		if _, ok := seen[node.ID]; !ok {
			order = append(order, node.ID)
		}
	}

	return order
}

func detectBackEdges(nodes []NodeDef, outgoing map[string][]EdgeDef) map[string]struct{} {
	backEdges := make(map[string]struct{})
	color := make(map[string]int, len(nodes))

	for _, node := range nodes {
		if color[node.ID] != colorWhite {
			continue
		}
		dfsBackEdges(node.ID, outgoing, color, backEdges)
	}

	return backEdges
}

func dfsBackEdges(start string, outgoing map[string][]EdgeDef, color map[string]int, backEdges map[string]struct{}) {
	type frame struct {
		nodeID  string
		edgeIdx int
	}

	stack := []frame{{nodeID: start}}
	color[start] = colorGray

	for len(stack) > 0 {
		top := &stack[len(stack)-1]
		edges := outgoing[top.nodeID]

		if top.edgeIdx >= len(edges) {
			color[top.nodeID] = colorBlack
			stack = stack[:len(stack)-1]
			continue
		}

		edge := edges[top.edgeIdx]
		top.edgeIdx++

		if color[edge.To] == colorGray {
			backEdges[edgeKey(edge.From, edge.To)] = struct{}{}
			continue
		}

		if color[edge.To] == colorWhite {
			color[edge.To] = colorGray
			stack = append(stack, frame{nodeID: edge.To})
		}
	}
}

func inSet(set map[string]struct{}, key string) bool {
	if len(set) == 0 {
		return false
	}
	_, ok := set[key]
	return ok
}

func edgeKey(from, to string) string {
	return from + "->" + to
}
