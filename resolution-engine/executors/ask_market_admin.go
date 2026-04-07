package executors

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

// AskMarketAdminConfig is the node config for ask_market_admin dispute steps.
type AskMarketAdminConfig struct {
	Prompt         string `json:"prompt"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
}

// AskMarketAdminExecutor waits for the market admin to submit a fallback
// dispute ruling. Uses the same indexer human-judgments API as human_judge,
// but restricted to the "market_admin" responder role.
type AskMarketAdminExecutor struct {
	inner *HumanJudgeExecutor
}

func NewAskMarketAdminExecutor(indexerURL string) *AskMarketAdminExecutor {
	return &AskMarketAdminExecutor{
		inner: NewHumanJudgeExecutor(indexerURL),
	}
}

func (e *AskMarketAdminExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	cfg, err := parseConfig[AskMarketAdminConfig](node.Config)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("ask_market_admin config: %w", err)
	}

	prompt := strings.TrimSpace(execCtx.Interpolate(cfg.Prompt))
	if prompt == "" {
		return dag.ExecutorResult{}, fmt.Errorf("ask_market_admin prompt is required")
	}
	if cfg.TimeoutSeconds <= 0 {
		cfg.TimeoutSeconds = 259200 // 72h default (longer than creator, last resort)
	}

	// Delegate to human_judge with market_admin-only responder
	wrappedNode := dag.NodeDef{
		ID:   node.ID,
		Type: "human_judge",
		Config: map[string]interface{}{
			"prompt":             prompt,
			"allowed_responders": []string{"market_admin"},
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
	result.Outputs["responder_role"] = "market_admin"
	result.Outputs["dispute_action"] = "admin_fallback_ruling"
	return result, nil
}

// SetPollInterval allows tests to override the polling interval.
func (e *AskMarketAdminExecutor) SetPollInterval(d time.Duration) {
	e.inner.PollInterval = d
}
