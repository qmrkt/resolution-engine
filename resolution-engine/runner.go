package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/question-market/resolution-engine/dag"
	"github.com/question-market/resolution-engine/executors"
)

// ResolutionPath indicates which blueprint produced the final result.
const (
	PathMain          = "main"
	PathDispute       = "dispute"
	PathAdminFallback = "admin_fallback"
)

// DualPathResult captures the outcome of a dual-path resolution attempt.
type DualPathResult struct {
	MainRun      *dag.RunState `json:"main_run"`
	DisputeRun   *dag.RunState `json:"dispute_run,omitempty"`
	PathUsed     string        `json:"path_used"`
	Outcome      string        `json:"outcome,omitempty"`
	EvidenceHash string        `json:"evidence_hash,omitempty"`
	Cancelled    bool          `json:"cancelled"`
	CancelReason string        `json:"cancel_reason,omitempty"`
}

// Runner executes resolution logic for markets and tracks in-flight runs.
type Runner struct {
	engine    *dag.Engine
	dataDir   string
	traceSink traceSink
	mu        sync.Mutex
	inFlight  map[int]bool // appID -> running
}

func NewRunner(anthropicKey string, indexerURL string, dataDir string, traceToken string) *Runner {
	engine := dag.NewEngine(nil)

	// Register all resolution executors
	engine.RegisterExecutor("api_fetch", executors.NewAPIFetchExecutor())
	engine.RegisterExecutor("market_evidence", executors.NewMarketEvidenceExecutor(indexerURL))
	engine.RegisterExecutor("llm_judge", executors.NewLLMJudgeExecutor(anthropicKey))
	engine.RegisterExecutor("human_judge", executors.NewHumanJudgeExecutor(indexerURL))
	engine.RegisterExecutor("ask_creator", executors.NewAskCreatorExecutor(indexerURL))
	engine.RegisterExecutor("ask_market_admin", executors.NewAskMarketAdminExecutor(indexerURL))
	engine.RegisterExecutor("submit_result", executors.NewSubmitResultExecutor())
	engine.RegisterExecutor("cancel_market", executors.NewCancelMarketExecutor())
	engine.RegisterExecutor("outcome_terminality", executors.NewOutcomeTerminalityExecutor())
	engine.RegisterExecutor("defer_resolution", executors.NewDeferResolutionExecutor())
	engine.RegisterExecutor("wait", executors.NewWaitExecutor())

	os.MkdirAll(dataDir, 0o755)

	return &Runner{
		engine:    engine,
		dataDir:   dataDir,
		traceSink: NewTraceEmitter(indexerURL, traceToken, nil),
		inFlight:  make(map[int]bool),
	}
}

func (r *Runner) TryStart(appID int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inFlight[appID] {
		return false
	}
	r.inFlight[appID] = true
	return true
}

func (r *Runner) Finish(appID int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.inFlight, appID)
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

func (r *Runner) RunBlueprint(appID int, resolutionLogicJSON []byte, inputs map[string]string, opts ...RunOptions) (*dag.RunState, error) {
	return r.executeResolutionWithInputs(appID, resolutionLogicJSON, inputs, firstRunOptions(opts...))
}

// TryResolve attempts to run resolution logic for a market.
// Returns immediately if the market is already in-flight.
func (r *Runner) TryResolve(appID int, resolutionLogicJSON []byte) {
	if !r.TryStart(appID) {
		return
	}

	go func() {
		defer r.Finish(appID)

		slog.Info("starting resolution", "component", "runner", slog.Int("app_id", appID))
		result, err := r.executeResolutionWithInputs(appID, resolutionLogicJSON, nil, RunOptions{
			Trace: &TraceMetadata{
				AppID:         appID,
				BlueprintPath: PathMain,
				Initiator:     "runner:manual",
			},
		})
		if err != nil {
			slog.Error("resolution failed", "component", "runner", slog.Int("app_id", appID), "error", err)
			return
		}

		// Save evidence to disk
		_, writeErr := r.WriteEvidence(appID, result)
		if writeErr != nil {
			slog.Error("failed to persist evidence", "component", "runner", slog.Int("app_id", appID), "error", writeErr)
		}

		slog.Info("resolution complete", "component", "runner", slog.Int("app_id", appID), slog.String("status", result.Status))

		// Check if submit_result produced an outcome
		if submission, ok := findSubmittedResolution(result); ok {
			slog.Info("market resolved",
				"component", "runner",
				slog.Int("app_id", appID),
				slog.String("outcome", submission.Outcome),
				slog.String("evidence_hash", submission.EvidenceHash),
			)
		} else if cancellation, ok := findRunAction(result, "cancelled"); ok {
			slog.Info("resolution cancelled",
				"component", "runner",
				slog.Int("app_id", appID),
				slog.String("reason", cancellation.Reason),
			)
		}
	}()
}

// TryResolveWithDispute attempts dual-path resolution: main blueprint first,
// then dispute blueprint if main fails, returns inconclusive, or is invalid.
// Returns immediately if the market is already in-flight.
func (r *Runner) TryResolveWithDispute(appID int, mainBlueprintJSON []byte, disputeBlueprintJSON []byte) {
	if !r.TryStart(appID) {
		return
	}

	go func() {
		defer r.Finish(appID)

		slog.Info("starting dual-path resolution", "component", "runner", slog.Int("app_id", appID))
		result, err := r.executeDualPath(appID, mainBlueprintJSON, disputeBlueprintJSON)
		if err != nil {
			slog.Error("dual-path resolution failed", "component", "runner", slog.Int("app_id", appID), "error", err)
			return
		}

		// Save evidence bundle (both runs) to disk
		_, writeErr := r.WriteEvidence(appID, result)
		if writeErr != nil {
			slog.Error("failed to persist dual-path evidence", "component", "runner", slog.Int("app_id", appID), "error", writeErr)
		}

		slog.Info("dual-path resolution complete",
			"component", "runner",
			slog.Int("app_id", appID),
			slog.String("path", result.PathUsed),
		)

		if result.Cancelled {
			slog.Warn("both paths failed, market cancelled",
				"component", "runner",
				slog.Int("app_id", appID),
				slog.String("reason", result.CancelReason),
			)
		} else if result.Outcome != "" {
			slog.Info("market resolved via dual path",
				"component", "runner",
				slog.Int("app_id", appID),
				slog.String("path", result.PathUsed),
				slog.String("outcome", result.Outcome),
				slog.String("evidence_hash", result.EvidenceHash),
			)
		}
	}()
}

// executeDualPath runs main blueprint, then dispute blueprint on failure.
func (r *Runner) executeDualPath(appID int, mainJSON []byte, disputeJSON []byte) (*DualPathResult, error) {
	result := &DualPathResult{}

	// Execute main path
	slog.Info("executing main blueprint", "component", "runner", slog.Int("app_id", appID))
	mainRun, mainErr := r.executeResolution(appID, mainJSON, RunOptions{
		Trace: &TraceMetadata{
			AppID:         appID,
			BlueprintPath: PathMain,
			Initiator:     "runner:manual",
		},
	})
	result.MainRun = mainRun

	mainSucceeded := mainErr == nil && mainRun != nil && isResolutionSuccessful(mainRun)

	if mainSucceeded {
		submission, _ := findSubmittedResolution(mainRun)
		result.PathUsed = PathMain
		result.Outcome = submission.Outcome
		result.EvidenceHash = submission.EvidenceHash
		return result, nil
	}

	if mainErr != nil {
		slog.Error("main path error", "component", "runner", slog.Int("app_id", appID), "error", mainErr)
	} else {
		slog.Info("main path inconclusive, falling back to dispute path", "component", "runner", slog.Int("app_id", appID))
	}

	// Execute dispute path
	slog.Info("executing dispute blueprint", "component", "runner", slog.Int("app_id", appID))
	disputeRun, disputeErr := r.executeResolution(appID, disputeJSON, RunOptions{
		Trace: &TraceMetadata{
			AppID:         appID,
			BlueprintPath: PathDispute,
			Initiator:     "runner:manual",
		},
	})
	result.DisputeRun = disputeRun

	disputeSucceeded := disputeErr == nil && disputeRun != nil && isResolutionSuccessful(disputeRun)

	if disputeSucceeded {
		submission, _ := findSubmittedResolution(disputeRun)
		result.PathUsed = PathDispute
		result.Outcome = submission.Outcome
		result.EvidenceHash = submission.EvidenceHash
		return result, nil
	}

	// Both paths failed
	result.PathUsed = PathDispute
	result.Cancelled = true
	reason := "both main and dispute paths failed to produce an outcome"
	if disputeErr != nil {
		reason = fmt.Sprintf("main failed, dispute error: %v", disputeErr)
	}
	result.CancelReason = reason
	return result, nil
}

// isResolutionSuccessful checks if a run produced a valid outcome via submit_result.
func isResolutionSuccessful(run *dag.RunState) bool {
	if run == nil {
		return false
	}
	if run.Status == "failed" || run.Status == "cancelled" {
		return false
	}
	if _, cancelled := findRunAction(run, "cancelled"); cancelled {
		return false
	}
	if _, deferred := findRunAction(run, "deferred"); deferred {
		return false
	}
	_, ok := findSubmittedResolution(run)
	return ok
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

func findSubmittedResolution(run *dag.RunState) (submittedResolution, bool) {
	var zero submittedResolution
	if run == nil {
		return zero, false
	}

	keys := sortedContextKeys(run.Context)
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
	for _, key := range sortedContextKeys(run.Context) {
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

func sortedContextKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (r *Runner) executeResolution(appID int, resolutionLogicJSON []byte, opts ...RunOptions) (*dag.RunState, error) {
	return r.executeResolutionWithInputs(appID, resolutionLogicJSON, nil, firstRunOptions(opts...))
}

func (r *Runner) executeResolutionWithInputs(appID int, resolutionLogicJSON []byte, extraInputs map[string]string, opts RunOptions) (*dag.RunState, error) {
	var bp dag.Blueprint
	if err := json.Unmarshal(resolutionLogicJSON, &bp); err != nil {
		return nil, fmt.Errorf("parse resolution logic: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 7*24*time.Hour)
	defer cancel()

	inputs := map[string]string{
		"market_app_id": fmt.Sprintf("%d", appID),
	}
	for key, value := range extraInputs {
		if strings.TrimSpace(key) == "" {
			continue
		}
		inputs[key] = value
	}

	var observer dag.RunObserver
	if opts.Trace != nil && r.traceSink != nil {
		observer = newTraceObserver(r.traceSink, *opts.Trace)
	}

	run, err := r.engine.ExecuteWithObserver(ctx, bp, inputs, observer)
	if err != nil {
		return run, fmt.Errorf("DAG execution: %w", err)
	}

	return run, nil
}

// EvidenceHash computes SHA-256 of run state for on-chain submission.
func EvidenceHash(run *dag.RunState) string {
	data, _ := json.Marshal(run)
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash)
}

type traceObserver struct {
	sink     traceSink
	metadata TraceMetadata
	mu       sync.Mutex
	revision int
}

func newTraceObserver(sink traceSink, metadata TraceMetadata) *traceObserver {
	return &traceObserver{
		sink: sink,
		metadata: TraceMetadata{
			AppID:         metadata.AppID,
			BlueprintPath: strings.TrimSpace(metadata.BlueprintPath),
			Initiator:     strings.TrimSpace(metadata.Initiator),
		},
	}
}

func (o *traceObserver) OnRunSnapshot(run *dag.RunState) {
	if o == nil || o.sink == nil || run == nil {
		return
	}

	o.mu.Lock()
	o.revision++
	revision := o.revision
	o.mu.Unlock()

	o.sink.Enqueue(TraceEnvelope{
		AppID:         o.metadata.AppID,
		BlueprintPath: o.metadata.BlueprintPath,
		Initiator:     o.metadata.Initiator,
		Revision:      revision,
		Run:           run,
	})
}

func firstRunOptions(opts ...RunOptions) RunOptions {
	if len(opts) == 0 {
		return RunOptions{}
	}
	return opts[0]
}
