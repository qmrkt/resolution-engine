package main

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/qmrkt/resolution-engine/dag"
)

type RunStatus string

const (
	RunStatusQueued    RunStatus = "queued"
	RunStatusRunning   RunStatus = "running"
	RunStatusWaiting   RunStatus = "waiting"
	RunStatusCompleted RunStatus = "completed"
	RunStatusFailed    RunStatus = "failed"
	RunStatusCancelled RunStatus = "cancelled"
)

type RunRequest struct {
	RunID         string
	AppID         int
	BlueprintJSON []byte
	Inputs        map[string]string
	BlueprintPath string
	Initiator     string
	CallbackURL   string
}

type RunResult struct {
	RunID         string          `json:"run_id"`
	AppID         int             `json:"app_id"`
	BlueprintPath string          `json:"blueprint_path"`
	Status        RunStatus       `json:"status"`
	Return        json.RawMessage `json:"return,omitempty"`
	Error         string          `json:"error,omitempty"`
	RunState      *dag.RunState   `json:"run_state,omitempty"`
}

func buildRunResult(req RunRequest, run *dag.RunState, err error) RunResult {
	result := RunResult{
		RunID:         req.RunID,
		AppID:         req.AppID,
		BlueprintPath: req.BlueprintPath,
		Status:        RunStatusCompleted,
		RunState:      run,
	}

	if err != nil {
		result.Error = err.Error()
		if errors.Is(err, context.Canceled) {
			result.Status = RunStatusCancelled
			return result
		}
		result.Status = RunStatusFailed
		return result
	}

	if run == nil {
		result.Status = RunStatusFailed
		result.Error = "run completed without run state"
		return result
	}

	if run.Status == "cancelled" {
		result.Status = RunStatusCancelled
		return result
	}
	if run.Status == "failed" {
		result.Status = RunStatusFailed
		result.Error = run.Error
	}
	result.Return = cloneJSONRawMessage(run.Return)

	return result
}
