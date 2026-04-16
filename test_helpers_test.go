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

func (m *mockExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	if m.err != nil {
		return dag.ExecutorResult{}, m.err
	}
	return dag.ExecutorResult{Outputs: m.outputs}, nil
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
