package executors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/qmrkt/resolution-engine/dag"
)

func (*AwaitSignalExecutor) ConfigSchema() json.RawMessage {
	return json.RawMessage(`{
  "type": "object",
  "required": ["signal_type"],
  "properties": {
    "reason": {"type": "string"},
    "signal_type": {"type": "string", "description": "Signal type the node waits on. Prefix matching applies: declaring \"foo\" accepts \"foo\" and \"foo.bar\"."},
    "correlation_key": {"type": "string", "description": "Scope the match to a specific waiter. Empty or \"auto\" derives \"<run_id>:<node_id>\"."},
    "timeout_seconds": {"type": "integer", "minimum": 0, "description": "Timeout in seconds before timeout_outputs fire. Defaults to 172800 (2 days) when unset."},
    "required_payload": {"type": "array", "items": {"type": "string"}, "description": "Keys that must be present and non-empty on the delivered signal."},
    "default_outputs": {"type": "object", "additionalProperties": {"type": "string"}, "description": "Seed outputs merged beneath the delivered signal payload on resume."},
    "timeout_outputs": {"type": "object", "additionalProperties": {"type": "string"}, "description": "Outputs emitted on timeout. Defaults to {\"status\": \"timeout\"}."}
  },
  "additionalProperties": false
}`)
}

func (*AwaitSignalExecutor) OutputKeys() []string {
	// Concrete keys are the union of default_outputs, signal payload, and
	// timeout_outputs — not decidable statically. Advertise only status,
	// which is always present.
	return []string{"status"}
}

// DefaultAwaitSignalTimeoutSeconds is the fallback deadline (2 days) applied
// when AwaitSignalConfig.TimeoutSeconds is unset.
const DefaultAwaitSignalTimeoutSeconds = 172800

// AwaitSignalConfig is the node config for await_signal steps.
type AwaitSignalConfig struct {
	Reason          string            `json:"reason,omitempty"`
	SignalType      string            `json:"signal_type"`
	CorrelationKey  string            `json:"correlation_key,omitempty"`
	TimeoutSeconds  int               `json:"timeout_seconds,omitempty"`
	RequiredPayload []string          `json:"required_payload,omitempty"`
	DefaultOutputs  map[string]string `json:"default_outputs,omitempty"`
	TimeoutOutputs  map[string]string `json:"timeout_outputs,omitempty"`
}

// AwaitSignalExecutor declares a signal-based suspension. The node parks until
// a matching Signal is delivered (resumes with merged DefaultOutputs + payload)
// or the timeout expires (resumes with TimeoutOutputs).
type AwaitSignalExecutor struct{}

func NewAwaitSignalExecutor() *AwaitSignalExecutor {
	return &AwaitSignalExecutor{}
}

// CanSuspend reports true for every await_signal node; the executor is
// unconditionally suspension-capable.
func (*AwaitSignalExecutor) CanSuspend(dag.NodeDef) bool { return true }

func (e *AwaitSignalExecutor) Execute(ctx context.Context, node dag.NodeDef, inv *dag.Invocation) (dag.ExecutorResult, error) {
	cfg, err := ParseConfig[AwaitSignalConfig](node.Config)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("await_signal config: %w", err)
	}
	signalType := strings.TrimSpace(cfg.SignalType)
	if signalType == "" {
		return dag.ExecutorResult{}, errors.New("await_signal signal_type is required")
	}

	timeoutSeconds := cfg.TimeoutSeconds
	if timeoutSeconds <= 0 {
		timeoutSeconds = DefaultAwaitSignalTimeoutSeconds
	}
	reason := strings.TrimSpace(cfg.Reason)
	if reason == "" {
		reason = "await signal"
	}
	correlationKey := AutoCorrelationKey(cfg.CorrelationKey, node.ID, inv)

	timeoutOutputs := cfg.TimeoutOutputs
	if len(timeoutOutputs) == 0 {
		timeoutOutputs = map[string]string{"status": "timeout"}
	}

	return dag.ExecutorResult{
		Suspend: &dag.Suspension{
			Kind:            dag.SuspensionKindSignal,
			Reason:          reason,
			SignalType:      signalType,
			CorrelationKey:  correlationKey,
			ResumeAtUnix:    time.Now().Add(time.Duration(timeoutSeconds) * time.Second).Unix(),
			RequiredPayload: NormalizeRequiredPayload(cfg.RequiredPayload),
			DefaultOutputs:  cfg.DefaultOutputs,
			TimeoutOutputs:  timeoutOutputs,
		},
	}, nil
}

// AutoCorrelationKey returns the configured correlation key, or
// `<runID>:<nodeID>` when the config value is empty or the literal "auto".
// Run identity travels on inv.Run.ID; no domain-specific names like
// market_app_id leak into the generic engine layer.
func AutoCorrelationKey(configured, nodeID string, inv *dag.Invocation) string {
	key := strings.TrimSpace(configured)
	if key != "" && key != "auto" {
		return key
	}
	runID := ""
	if inv != nil {
		runID = strings.TrimSpace(inv.Run.ID)
	}
	return fmt.Sprintf("%s:%s", runID, nodeID)
}

// NormalizeRequiredPayload trims, deduplicates, and sorts required payload
// keys so two equivalent config shapes produce the same persisted record.
func NormalizeRequiredPayload(required []string) []string {
	if len(required) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(required))
	normalized := make([]string, 0, len(required))
	for _, key := range required {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		normalized = append(normalized, key)
	}
	sort.Strings(normalized)
	return normalized
}
