package executors

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/question-market/resolution-engine/dag"
)

// SubmitResultConfig is the node config for submit_result steps.
type SubmitResultConfig struct {
	OutcomeKey string `json:"outcome_key,omitempty"` // context key holding the outcome index (default: auto-detect)
}

// SubmitResultExecutor submits a resolution proposal on-chain.
// In V1, this collects the outcome and evidence hash into context
// for the runner to submit via algosdk.
type SubmitResultExecutor struct{}

func NewSubmitResultExecutor() *SubmitResultExecutor {
	return &SubmitResultExecutor{}
}

func (e *SubmitResultExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	cfg, _ := parseConfig[SubmitResultConfig](node.Config)

	// Find the outcome index from context
	outcome := ""
	if cfg.OutcomeKey != "" {
		outcome = execCtx.Get(cfg.OutcomeKey)
	}

	// Auto-detect: collect all *.outcome keys, reject if they disagree
	if outcome == "" {
		snap := execCtx.Snapshot()
		type candidate struct {
			key, value string
		}
		var candidates []candidate
		for k, v := range snap {
			if len(k) > 8 && k[len(k)-8:] == ".outcome" && v != "" && v != "inconclusive" {
				candidates = append(candidates, candidate{k, v})
			}
		}
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].key < candidates[j].key
		})
		if len(candidates) > 0 {
			outcome = candidates[0].value
			for _, c := range candidates[1:] {
				if c.value != outcome {
					keys := make([]string, len(candidates))
					for i, cc := range candidates {
						keys[i] = fmt.Sprintf("%s=%s", cc.key, cc.value)
					}
					return dag.ExecutorResult{}, fmt.Errorf("ambiguous outcome: upstream nodes disagree: %v", keys)
				}
			}
		}
	}

	if outcome == "" || outcome == "inconclusive" {
		return dag.ExecutorResult{Outputs: map[string]string{
			"status": "failed",
			"error":  "no outcome determined",
		}}, fmt.Errorf("no outcome determined — cannot submit")
	}

	// Compute evidence hash from full context snapshot
	snap := execCtx.Snapshot()
	evidenceJSON, _ := json.Marshal(snap)
	hash := sha256.Sum256(evidenceJSON)

	return dag.ExecutorResult{Outputs: map[string]string{
		"status":        "success",
		"outcome":       outcome,
		"evidence_hash": fmt.Sprintf("%x", hash),
		"submitted":     "true",
	}}, nil
}

// CancelMarketConfig is the node config for cancel_market steps.
type CancelMarketConfig struct {
	Reason string `json:"reason,omitempty"`
}

// CancelMarketExecutor cancels a market on-chain.
// In V1, this sets context flags for the runner to submit via algosdk.
type CancelMarketExecutor struct{}

func NewCancelMarketExecutor() *CancelMarketExecutor {
	return &CancelMarketExecutor{}
}

func (e *CancelMarketExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	cfg, _ := parseConfig[CancelMarketConfig](node.Config)
	reason := cfg.Reason
	if reason == "" {
		reason = "resolution failed"
	}

	return dag.ExecutorResult{Outputs: map[string]string{
		"status":    "success",
		"action":    "cancel",
		"reason":    reason,
		"cancelled": "true",
	}}, nil
}
