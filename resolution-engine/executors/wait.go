package executors

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

// WaitConfig is the node config for wait steps.
type WaitConfig struct {
	DurationSeconds int    `json:"duration_seconds"`
	Mode            string `json:"mode,omitempty"`
	StartFrom       string `json:"start_from,omitempty"`
}

// WaitExecutor sleeps for a configured duration.
type WaitExecutor struct{}

func NewWaitExecutor() *WaitExecutor {
	return &WaitExecutor{}
}

func (e *WaitExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	cfg, err := parseConfig[WaitConfig](node.Config)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("wait config: %w", err)
	}

	mode := strings.TrimSpace(cfg.Mode)
	if mode == "" {
		mode = "sleep"
	}
	duration := time.Duration(cfg.DurationSeconds) * time.Second
	if duration < 0 {
		duration = 0
	}

	if mode == "defer" {
		return executeDeferredWait(cfg, execCtx)
	}

	select {
	case <-time.After(duration):
		return dag.ExecutorResult{Outputs: map[string]string{
			"status": "success",
			"waited": fmt.Sprintf("%d", cfg.DurationSeconds),
			"mode":   mode,
		}}, nil
	case <-ctx.Done():
		return dag.ExecutorResult{}, ctx.Err()
	}
}

func executeDeferredWait(cfg WaitConfig, execCtx *dag.Context) (dag.ExecutorResult, error) {
	startFrom := strings.TrimSpace(cfg.StartFrom)
	if startFrom == "" {
		startFrom = "deadline"
	}
	if startFrom == "now" {
		return dag.ExecutorResult{}, fmt.Errorf("wait defer mode requires a stable start_from anchor")
	}

	nowTS, err := parseWaitTimestamp(execCtx, "market.now_ts")
	if err != nil {
		return dag.ExecutorResult{}, err
	}
	anchorKey := "market." + startFrom
	anchorTS, err := parseWaitTimestamp(execCtx, anchorKey)
	if err != nil {
		return dag.ExecutorResult{}, err
	}

	readyAt := anchorTS + int64(cfg.DurationSeconds)
	outputs := map[string]string{
		"mode":              "defer",
		"start_from":        startFrom,
		"anchor_ts":         strconv.FormatInt(anchorTS, 10),
		"ready_at":          strconv.FormatInt(readyAt, 10),
		"waited":            fmt.Sprintf("%d", cfg.DurationSeconds),
		"remaining_seconds": "0",
	}
	if nowTS >= readyAt {
		outputs["status"] = "success"
		return dag.ExecutorResult{Outputs: outputs}, nil
	}

	outputs["status"] = "waiting"
	outputs["waiting"] = "true"
	outputs["remaining_seconds"] = strconv.FormatInt(readyAt-nowTS, 10)
	return dag.ExecutorResult{Outputs: outputs}, nil
}

func parseWaitTimestamp(execCtx *dag.Context, key string) (int64, error) {
	raw := strings.TrimSpace(execCtx.Get(key))
	if raw == "" {
		return 0, fmt.Errorf("%s is required for wait defer mode", key)
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return value, nil
}
