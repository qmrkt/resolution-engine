package executors

import (
	"context"

	"github.com/question-market/resolution-engine/dag"
)

type DeferResolutionConfig struct {
	Reason string `json:"reason,omitempty"`
}

type DeferResolutionExecutor struct{}

func NewDeferResolutionExecutor() *DeferResolutionExecutor {
	return &DeferResolutionExecutor{}
}

func (e *DeferResolutionExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	cfg, _ := parseConfig[DeferResolutionConfig](node.Config)
	reason := cfg.Reason
	if reason == "" {
		reason = "resolution deferred"
	}

	return dag.ExecutorResult{Outputs: map[string]string{
		"status":   "success",
		"action":   "defer",
		"deferred": "true",
		"reason":   reason,
	}}, nil
}
