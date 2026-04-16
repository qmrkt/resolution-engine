package main

import (
	"context"
	"errors"
	"strings"

	"github.com/qmrkt/resolution-engine/dag"
)

type RunStatus string

type RunAction string

const (
	RunStatusAccepted  RunStatus = "accepted"
	RunStatusQueued    RunStatus = "queued"
	RunStatusRunning   RunStatus = "running"
	RunStatusWaiting   RunStatus = "waiting"
	RunStatusCompleted RunStatus = "completed"
	RunStatusFailed    RunStatus = "failed"
	RunStatusCancelled RunStatus = "cancelled"
)

const (
	RunActionNone            RunAction = "none"
	RunActionPropose         RunAction = "propose"
	RunActionFinalizeDispute RunAction = "finalize_dispute"
	RunActionCancelMarket    RunAction = "cancel_market"
	RunActionDefer           RunAction = "defer"
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
	RunID         string        `json:"run_id"`
	AppID         int           `json:"app_id"`
	BlueprintPath string        `json:"blueprint_path"`
	Status        RunStatus     `json:"status"`
	Action        RunAction     `json:"action"`
	Outcome       string        `json:"outcome,omitempty"`
	EvidenceHash  string        `json:"evidence_hash,omitempty"`
	Reason        string        `json:"reason,omitempty"`
	Error         string        `json:"error,omitempty"`
	RunState      *dag.RunState `json:"run_state,omitempty"`
}

func buildRunResult(req RunRequest, run *dag.RunState, err error) RunResult {
	result := RunResult{
		RunID:         strings.TrimSpace(req.RunID),
		AppID:         req.AppID,
		BlueprintPath: strings.TrimSpace(req.BlueprintPath),
		Status:        RunStatusCompleted,
		Action:        RunActionNone,
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

	if submission, ok := findSubmittedResolution(run); ok {
		result.Outcome = submission.Outcome
		result.EvidenceHash = submission.EvidenceHash
		if result.BlueprintPath == PathDispute {
			result.Action = RunActionFinalizeDispute
		} else {
			result.Action = RunActionPropose
		}
	}
	if cancellation, ok := findRunAction(run, "cancelled"); ok {
		result.Action = RunActionCancelMarket
		result.Reason = cancellation.Reason
	}
	if deferred, ok := findRunAction(run, "deferred"); ok {
		result.Action = RunActionDefer
		result.Reason = deferred.Reason
	}

	return result
}

func validateTerminalRunResult(result RunResult) error {
	if result.Status != RunStatusCompleted && result.Status != RunStatusFailed && result.Status != RunStatusCancelled {
		return nil
	}

	switch result.Action {
	case RunActionPropose, RunActionFinalizeDispute:
		if strings.TrimSpace(result.Outcome) == "" {
			return errors.New("terminal result missing outcome")
		}
		if strings.TrimSpace(result.EvidenceHash) == "" {
			return errors.New("terminal result missing evidence_hash")
		}
	case RunActionCancelMarket, RunActionDefer:
		if strings.TrimSpace(result.Reason) == "" {
			return errors.New("terminal result missing reason")
		}
	case RunActionNone:
		return nil
	default:
		return errors.New("terminal result has unknown action")
	}

	return nil
}
