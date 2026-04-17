package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/qmrkt/resolution-engine/dag"
	"github.com/qmrkt/resolution-engine/executors"
)

// Runner wires the DAG engine with registered executors and the durable data
// directory used for evidence persistence.
type Runner struct {
	engine    *dag.Engine
	dataDir   string
	agentExec *executors.AgentLoopExecutor
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

	agentExec := executors.NewAgentLoopExecutorWithConfig(llmConfig, engine)
	engine.RegisterExecutor("api_fetch", executors.NewAPIFetchExecutor())
	engine.RegisterExecutor("llm_call", executors.NewLLMCallExecutorWithConfig(llmConfig))
	engine.RegisterExecutor("agent_loop", agentExec)
	engine.RegisterExecutor("return", executors.NewReturnExecutor())
	engine.RegisterExecutor("await_signal", executors.NewAwaitSignalExecutor())
	engine.RegisterExecutor("wait", executors.NewWaitExecutor())
	engine.RegisterExecutor("cel_eval", executors.NewCelEvalExecutor())
	engine.RegisterExecutor("map", executors.NewMapExecutor(engine))
	engine.RegisterExecutor("gadget", executors.NewGadgetExecutor(engine, adaptBlueprintValidation))
	engine.RegisterExecutor("validate_blueprint", executors.NewValidateBlueprintExecutor(adaptBlueprintValidation))

	_ = os.MkdirAll(dataDir, 0o755)

	return &Runner{
		engine:    engine,
		dataDir:   dataDir,
		agentExec: agentExec,
	}
}

// AgentExecutor returns the registered agent_loop executor so the durable
// manager can install its async signal sink.
func (r *Runner) AgentExecutor() *executors.AgentLoopExecutor {
	if r == nil {
		return nil
	}
	return r.agentExec
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
