package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/qmrkt/resolution-engine/dag"
)

func (*WaitExecutor) ConfigSchema() json.RawMessage {
	return json.RawMessage(`{
  "type": "object",
  "properties": {
    "duration_seconds": {"type": "integer", "minimum": 0, "description": "Wall-clock wait in seconds. Required when mode is sleep (default)."},
    "mode": {"type": "string", "enum": ["sleep", "defer"], "default": "sleep", "description": "sleep waits for duration_seconds. defer waits until market.<start_from> + duration_seconds."},
    "start_from": {"type": "string", "enum": ["deadline", "resolution_pending_since"], "description": "Anchor key under market.* for defer mode. Cannot be \"now\"."},
    "max_inline_seconds": {"type": "integer", "minimum": -1, "maximum": 300, "description": "Per-node inline cap. <0 forces suspension; 0 uses DefaultMaxInlineWaitSeconds (20s); >0 is an explicit cap bounded by MaxInlineWaitSecondsCap (300s)."}
  },
  "additionalProperties": false
}`)
}

func (*WaitExecutor) OutputKeys() []string {
	// Sleep mode emits status, waited, mode. Defer mode adds start_from,
	// anchor_ts, ready_at, remaining_seconds. Advertise the union.
	return []string{"status", "waited", "mode", "start_from", "anchor_ts", "ready_at", "remaining_seconds"}
}

// DefaultMaxInlineWaitSeconds is the upper bound (inclusive) for
// WaitExecutor's inline fast path. Waits shorter than this run via a local
// time.After sleep and return outputs synchronously — no suspension, no
// engine round-trip. Anything longer goes through the durable suspension
// path. Per-node overrides live in WaitConfig.MaxInlineSeconds.
const DefaultMaxInlineWaitSeconds = 20

// MaxInlineWaitSecondsCap is the hard ceiling for a per-node
// WaitConfig.MaxInlineSeconds override. Anything over this would pin an
// executor goroutine long enough that blueprint validation should reject
// it and force the durable suspension path.
const MaxInlineWaitSecondsCap = 300

// WaitConfig is the node config for wait steps.
type WaitConfig struct {
	DurationSeconds int    `json:"duration_seconds"`
	Mode            string `json:"mode,omitempty"`
	StartFrom       string `json:"start_from,omitempty"`
	// MaxInlineSeconds caps the inline sleep duration. When <= 0 the
	// DefaultMaxInlineWaitSeconds constant is used. Short waits within
	// the cap run synchronously in the executor goroutine regardless of
	// which engine called it, letting sub-blueprints (gadget children,
	// map inline bodies, agent blueprint tools) include brief pauses.
	MaxInlineSeconds int `json:"max_inline_seconds,omitempty"`
}

// WaitExecutor sleeps inline for short durations and declares a timer-based
// suspension for longer ones.
//
// Zero-duration sleeps and defer-mode waits whose anchor has already elapsed
// return final outputs directly (no Suspend). Durations within the inline
// cap (DefaultMaxInlineWaitSeconds, overridable per-node) run via an
// interruptible time.After — both the in-memory and durable engines accept
// this path. Durations over the cap return a timer Suspension; the durable
// engine parks the node and the in-memory engine rejects it.
type WaitExecutor struct{}

func NewWaitExecutor() *WaitExecutor {
	return &WaitExecutor{}
}

// CanSuspend reports whether the wait node would park itself. Sleep-mode
// waits within the inline cap (or zero duration) resolve synchronously.
// Defer mode depends on market.now_ts and the anchor — not decidable
// statically — so it is treated as capable.
func (e *WaitExecutor) CanSuspend(node dag.NodeDef) bool {
	cfg, err := ParseConfig[WaitConfig](node.Config)
	if err != nil {
		return true
	}
	mode := strings.TrimSpace(cfg.Mode)
	if mode == "" {
		mode = "sleep"
	}
	if mode != "sleep" {
		return true
	}
	duration := cfg.DurationSeconds
	if duration <= 0 {
		return false
	}
	return duration > resolveInlineWaitCap(cfg)
}

func (e *WaitExecutor) Execute(ctx context.Context, node dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	cfg, err := ParseConfig[WaitConfig](node.Config)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("wait config: %w", err)
	}

	mode := strings.TrimSpace(cfg.Mode)
	if mode == "" {
		mode = "sleep"
	}
	duration := cfg.DurationSeconds
	if duration < 0 {
		duration = 0
	}

	if mode == "defer" {
		return deferWait(ctx, cfg, inv)
	}

	outputs := map[string]string{
		"status": "success",
		"waited": strconv.Itoa(duration),
		"mode":   mode,
	}
	if duration == 0 {
		return dag.ExecutorResult{Outputs: outputs}, nil
	}
	if duration <= resolveInlineWaitCap(cfg) {
		if err := inlineSleep(ctx, time.Duration(duration)*time.Second); err != nil {
			return dag.ExecutorResult{}, err
		}
		return dag.ExecutorResult{Outputs: outputs}, nil
	}
	return dag.ExecutorResult{
		Suspend: &dag.Suspension{
			Kind:         dag.SuspensionKindTimer,
			Reason:       "sleep",
			ResumeAtUnix: time.Now().Add(time.Duration(duration) * time.Second).Unix(),
			Outputs:      outputs,
		},
	}, nil
}

func deferWait(ctx context.Context, cfg WaitConfig, inv *dag.Invocation) (dag.ExecutorResult, error) {
	startFrom := strings.TrimSpace(cfg.StartFrom)
	if startFrom == "" {
		startFrom = "deadline"
	}
	if startFrom == "now" {
		return dag.ExecutorResult{}, fmt.Errorf("wait defer mode requires a stable start_from anchor")
	}

	nowTS, err := parseWaitTimestamp(inv, "inputs.market.now_ts")
	if err != nil {
		return dag.ExecutorResult{}, err
	}
	anchorKey := "inputs.market." + startFrom
	anchorTS, err := parseWaitTimestamp(inv, anchorKey)
	if err != nil {
		return dag.ExecutorResult{}, err
	}

	readyAt := anchorTS + int64(cfg.DurationSeconds)
	outputs := map[string]string{
		"status":            "success",
		"mode":              "defer",
		"start_from":        startFrom,
		"anchor_ts":         strconv.FormatInt(anchorTS, 10),
		"ready_at":          strconv.FormatInt(readyAt, 10),
		"waited":            strconv.Itoa(cfg.DurationSeconds),
		"remaining_seconds": "0",
	}
	if nowTS >= readyAt {
		return dag.ExecutorResult{Outputs: outputs}, nil
	}

	remaining := readyAt - nowTS
	if remaining <= int64(resolveInlineWaitCap(cfg)) {
		if err := inlineSleep(ctx, time.Duration(remaining)*time.Second); err != nil {
			return dag.ExecutorResult{}, err
		}
		return dag.ExecutorResult{Outputs: outputs}, nil
	}

	return dag.ExecutorResult{
		Suspend: &dag.Suspension{
			Kind:         dag.SuspensionKindTimer,
			Reason:       "defer",
			ResumeAtUnix: readyAt,
			Outputs:      outputs,
		},
	}, nil
}

// resolveInlineWaitCap returns the effective inline-wait cap in seconds.
// Semantics of cfg.MaxInlineSeconds:
//
//	< 0 : force suspension for any positive duration (tests, callers that
//	      need to exercise the durable path).
//	= 0 : use DefaultMaxInlineWaitSeconds.
//	> 0 : explicit cap.
func resolveInlineWaitCap(cfg WaitConfig) int {
	if cfg.MaxInlineSeconds < 0 {
		return 0
	}
	if cfg.MaxInlineSeconds > 0 {
		return cfg.MaxInlineSeconds
	}
	return DefaultMaxInlineWaitSeconds
}

// inlineSleep blocks for d or until ctx is cancelled, returning the context
// error in the latter case.
func inlineSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func parseWaitTimestamp(inv *dag.Invocation, key string) (int64, error) {
	raw := strings.TrimSpace(inv.Lookup(key))
	if raw == "" {
		return 0, fmt.Errorf("%s is required for wait defer mode", key)
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return value, nil
}
