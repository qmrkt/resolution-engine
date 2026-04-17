package main

import (
	"encoding/json"
	"fmt"

	"github.com/qmrkt/resolution-engine/dag"
)

// duplicateRunError is returned when a new submission collides with an already
// active run for the same app.
type duplicateRunError struct {
	RunID string
}

func (e *duplicateRunError) Error() string {
	return fmt.Sprintf("run already active: %s", e.RunID)
}

func cloneRunResult(result RunResult) RunResult {
	cloned := result
	cloned.Return = cloneJSONRawMessage(result.Return)
	if result.RunState != nil {
		cloned.RunState = cloneDAGRunState(result.RunState)
	}
	return cloned
}

func cloneDAGRunState(run *dag.RunState) *dag.RunState {
	if run == nil {
		return nil
	}
	cloned := *run
	cloned.Return = cloneJSONRawMessage(run.Return)
	cloned.Definition = cloneBlueprint(run.Definition)
	cloned.Inputs = cloneStringMap(run.Inputs)
	cloned.NodeStates = cloneNodeStateMap(run.NodeStates)
	cloned.EdgeTraversals = cloneIntMap(run.EdgeTraversals)
	cloned.NodeTraces = cloneNodeTraces(run.NodeTraces)
	return &cloned
}

func cloneBlueprint(bp dag.Blueprint) dag.Blueprint {
	cloned := bp
	if bp.Nodes != nil {
		cloned.Nodes = append([]dag.NodeDef(nil), bp.Nodes...)
		for i := range cloned.Nodes {
			cloned.Nodes[i].Config = cloneJSONValue(cloned.Nodes[i].Config)
		}
	}
	if bp.Edges != nil {
		cloned.Edges = append([]dag.EdgeDef(nil), bp.Edges...)
	}
	if bp.Inputs != nil {
		cloned.Inputs = append([]dag.InputDef(nil), bp.Inputs...)
	}
	if bp.Budget != nil {
		budget := *bp.Budget
		if bp.Budget.PerNode != nil {
			budget.PerNode = make(map[string]dag.NodeBud, len(bp.Budget.PerNode))
			for key, value := range bp.Budget.PerNode {
				budget.PerNode[key] = value
			}
		}
		cloned.Budget = &budget
	}
	return cloned
}

func cloneJSONValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var cloned interface{}
	if err := json.Unmarshal(data, &cloned); err != nil {
		return value
	}
	return cloned
}

func cloneJSONRawMessage(value json.RawMessage) json.RawMessage {
	if len(value) == 0 {
		return nil
	}
	return append(json.RawMessage(nil), value...)
}

func cloneJSONRawMessagePtr(value *json.RawMessage) *json.RawMessage {
	if value == nil {
		return nil
	}
	cloned := cloneJSONRawMessage(*value)
	return &cloned
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneIntMap(values map[string]int) map[string]int {
	if values == nil {
		return nil
	}
	cloned := make(map[string]int, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneNodeStateMap(values map[string]dag.NodeState) map[string]dag.NodeState {
	if values == nil {
		return nil
	}
	cloned := make(map[string]dag.NodeState, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneNodeTraces(values []dag.NodeTrace) []dag.NodeTrace {
	if values == nil {
		return nil
	}
	cloned := make([]dag.NodeTrace, len(values))
	for i, trace := range values {
		cloned[i] = trace
		cloned[i].InputSnapshot = cloneStringMap(trace.InputSnapshot)
		cloned[i].Outputs = cloneStringMap(trace.Outputs)
	}
	return cloned
}
