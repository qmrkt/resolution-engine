package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/qmrkt/resolution-engine/dag"
)

// mockExecutor returns fixed outputs (or an error) for any node it runs.
// Used throughout the main-package test suite wherever a lightweight stub
// executor is enough.
type mockExecutor struct {
	outputs map[string]string
	err     error
}

func (m *mockExecutor) Execute(ctx context.Context, node dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	if m.err != nil {
		return dag.ExecutorResult{}, m.err
	}
	return dag.ExecutorResult{Outputs: m.outputs}, nil
}

// testResultValue reads `<nodeID>.<field>` from a RunState's Results.
func testResultValue(run *dag.RunState, nodeID, field string) string {
	if run == nil || run.Results == nil {
		return ""
	}
	v, _ := run.Results.Get(nodeID, field)
	return v
}

// testResultHistoryJSON returns the back-edge history of a node as a JSON
// array string. Empty string when there is no history.
func testResultHistoryJSON(run *dag.RunState, nodeID string) string {
	if run == nil || run.Results == nil {
		return ""
	}
	history := run.Results.History(nodeID)
	if len(history) == 0 {
		return ""
	}
	data, err := json.Marshal(history)
	if err != nil {
		return ""
	}
	return string(data)
}

// runBlueprintSync executes a blueprint JSON payload through the runner's
// engine in-process with no durable checkpointing. Used by e2e tests that want
// synchronous end-to-end validation of executors without the durable queue.
func runBlueprintSync(runner *Runner, appID int, blueprintJSON []byte, extraInputs map[string]string) (*dag.RunState, error) {
	var bp dag.Blueprint
	if err := json.Unmarshal(blueprintJSON, &bp); err != nil {
		return nil, fmt.Errorf("parse blueprint: %w", err)
	}
	inputs := map[string]string{"market_app_id": fmt.Sprintf("%d", appID)}
	for key, value := range extraInputs {
		inputs[key] = value
	}
	return runner.engine.Execute(context.Background(), bp, inputs)
}
