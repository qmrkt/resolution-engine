package executors

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

// AskCreatorConfig is the node config for ask_creator dispute steps.
type AskCreatorConfig struct {
	Prompt         string `json:"prompt"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
}

// AskCreatorExecutor waits for the market creator to submit a dispute ruling.
// Uses the same indexer human-judgments API as human_judge, but restricted to
// the "creator" responder role.
type AskCreatorExecutor struct {
	inner *HumanJudgeExecutor
}

func NewAskCreatorExecutor(indexerURL string) *AskCreatorExecutor {
	return &AskCreatorExecutor{
		inner: NewHumanJudgeExecutor(indexerURL),
	}
}

func (e *AskCreatorExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	cfg, err := parseConfig[AskCreatorConfig](node.Config)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("ask_creator config: %w", err)
	}

	prompt := strings.TrimSpace(execCtx.Interpolate(cfg.Prompt))
	if prompt == "" {
		return dag.ExecutorResult{}, fmt.Errorf("ask_creator prompt is required")
	}
	if cfg.TimeoutSeconds <= 0 {
		cfg.TimeoutSeconds = 172800 // 48h default
	}

	// Delegate to human_judge with creator-only responder
	wrappedNode := dag.NodeDef{
		ID:   node.ID,
		Type: "human_judge",
		Config: map[string]interface{}{
			"prompt":             prompt,
			"allowed_responders": []string{"creator"},
			"timeout_seconds":    cfg.TimeoutSeconds,
			"require_reason":     true,
			"allow_cancel":       true,
		},
		OnError: node.OnError,
	}

	result, err := e.inner.Execute(ctx, wrappedNode, execCtx)
	if err != nil {
		return result, err
	}

	// Tag the resolution path
	result.Outputs["responder_role"] = "creator"
	result.Outputs["dispute_action"] = "creator_ruling"
	return result, nil
}

// SetPollInterval allows tests to override the polling interval.
func (e *AskCreatorExecutor) SetPollInterval(d time.Duration) {
	e.inner.PollInterval = d
}
