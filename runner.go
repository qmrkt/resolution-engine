package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/qmrkt/resolution-engine/dag"
	"github.com/qmrkt/resolution-engine/executors"
)

const (
	PathMain    = "main"
	PathDispute = "dispute"
)

// Runner wires the DAG engine with registered executors and the durable data
// directory used for evidence persistence.
type Runner struct {
	engine  *dag.Engine
	dataDir string
}

type submittedResolution struct {
	NodeID       string
	Outcome      string
	EvidenceHash string
}

type runAction struct {
	NodeID string
	Reason string
}

func adaptBlueprintValidation(bp dag.Blueprint, rawJSON []byte) executors.BlueprintValidationResult {
	result := ValidateResolutionBlueprint(bp, rawJSON)
	issues := make([]executors.BlueprintValidationIssue, 0, len(result.Issues))
	for _, issue := range result.Issues {
		issues = append(issues, executors.BlueprintValidationIssue{
			Code:     issue.Code,
			Message:  issue.Message,
			Target:   issue.Target,
			Severity: issue.Severity,
		})
	}
	return executors.BlueprintValidationResult{
		Valid:  result.Valid,
		Issues: issues,
	}
}

func NewRunner(llmConfig executors.LLMCallExecutorConfig, dataDir string) *Runner {
	engine := dag.NewEngine(nil)

	engine.RegisterExecutor("api_fetch", executors.NewAPIFetchExecutor())
	engine.RegisterExecutor("llm_call", executors.NewLLMCallExecutorWithConfig(llmConfig))
	engine.RegisterExecutor("agent_loop", executors.NewAgentLoopExecutorWithConfig(llmConfig, engine))
	engine.RegisterExecutor("submit_result", executors.NewSubmitResultExecutor())
	engine.RegisterExecutor("cancel_market", executors.NewCancelMarketExecutor())
	engine.RegisterExecutor("defer_resolution", executors.NewDeferResolutionExecutor())
	engine.RegisterExecutor("wait", executors.NewWaitExecutor())
	engine.RegisterExecutor("cel_eval", executors.NewCelEvalExecutor())
	engine.RegisterExecutor("map", executors.NewMapExecutor(engine))
	engine.RegisterExecutor("gadget", executors.NewGadgetExecutor(engine, adaptBlueprintValidation))
	engine.RegisterExecutor("validate_blueprint", executors.NewValidateBlueprintExecutor(adaptBlueprintValidation))

	_ = os.MkdirAll(dataDir, 0o755)

	return &Runner{
		engine:  engine,
		dataDir: dataDir,
	}
}

func (r *Runner) WriteEvidence(appID int, payload interface{}) (string, error) {
	evidencePath := filepath.Join(r.dataDir, fmt.Sprintf("evidence_%d.json", appID))
	evidenceJSON, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal evidence payload: %w", err)
	}
	if err := os.WriteFile(evidencePath, evidenceJSON, 0o644); err != nil {
		return "", fmt.Errorf("write evidence payload: %w", err)
	}
	return evidencePath, nil
}

func findSubmittedResolution(run *dag.RunState) (submittedResolution, bool) {
	var zero submittedResolution
	if run == nil {
		return zero, false
	}

	keys := executors.SortedContextKeys(run.Context)
	for _, key := range keys {
		if !strings.HasSuffix(key, ".submitted") {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(run.Context[key]), "true") {
			continue
		}

		nodeID := strings.TrimSuffix(key, ".submitted")
		outcome := strings.TrimSpace(run.Context[nodeID+".outcome"])
		if outcome == "" || outcome == "inconclusive" {
			continue
		}

		return submittedResolution{
			NodeID:       nodeID,
			Outcome:      outcome,
			EvidenceHash: strings.TrimSpace(run.Context[nodeID+".evidence_hash"]),
		}, true
	}

	for _, key := range keys {
		if key != "submit.outcome" {
			continue
		}
		outcome := strings.TrimSpace(run.Context[key])
		if outcome == "" || outcome == "inconclusive" {
			continue
		}

		return submittedResolution{
			NodeID:       "submit",
			Outcome:      outcome,
			EvidenceHash: strings.TrimSpace(run.Context["submit.evidence_hash"]),
		}, true
	}

	return zero, false
}

func findRunAction(run *dag.RunState, flag string) (runAction, bool) {
	var zero runAction
	if run == nil || strings.TrimSpace(flag) == "" {
		return zero, false
	}

	suffix := "." + strings.TrimSpace(flag)
	for _, key := range executors.SortedContextKeys(run.Context) {
		if !strings.HasSuffix(key, suffix) {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(run.Context[key]), "true") {
			continue
		}

		nodeID := strings.TrimSuffix(key, suffix)
		return runAction{
			NodeID: nodeID,
			Reason: strings.TrimSpace(run.Context[nodeID+".reason"]),
		}, true
	}

	return zero, false
}
