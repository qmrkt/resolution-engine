package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/question-market/resolution-engine/dag"
	"github.com/question-market/resolution-engine/executors"
)

const (
	PathMain    = "main"
	PathDispute = "dispute"
)

// Runner executes resolution blueprints and persists evidence/traces.
type Runner struct {
	engine    *dag.Engine
	dataDir   string
	traceSink traceSink
	baseCtx   context.Context
	mu        sync.Mutex
}

func NewRunner(llmConfig executors.LLMJudgeExecutorConfig, indexerURL string, dataDir string, traceToken string) *Runner {
	engine := dag.NewEngine(nil)

	engine.RegisterExecutor("api_fetch", executors.NewAPIFetchExecutor())
	engine.RegisterExecutor("market_evidence", executors.NewMarketEvidenceExecutor(indexerURL))
	engine.RegisterExecutor("llm_judge", executors.NewLLMJudgeExecutorWithConfig(llmConfig))
	engine.RegisterExecutor("human_judge", executors.NewHumanJudgeExecutor(indexerURL))
	engine.RegisterExecutor("ask_creator", executors.NewAskCreatorExecutor(indexerURL))
	engine.RegisterExecutor("ask_market_admin", executors.NewAskMarketAdminExecutor(indexerURL))
	engine.RegisterExecutor("submit_result", executors.NewSubmitResultExecutor())
	engine.RegisterExecutor("cancel_market", executors.NewCancelMarketExecutor())
	engine.RegisterExecutor("outcome_terminality", executors.NewOutcomeTerminalityExecutor())
	engine.RegisterExecutor("defer_resolution", executors.NewDeferResolutionExecutor())
	engine.RegisterExecutor("wait", executors.NewWaitExecutor())

	_ = os.MkdirAll(dataDir, 0o755)

	return &Runner{
		engine:    engine,
		dataDir:   dataDir,
		traceSink: NewTraceEmitter(indexerURL, traceToken, nil),
		baseCtx:   context.Background(),
	}
}

func (r *Runner) SetContext(ctx context.Context) {
	if r == nil || ctx == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.baseCtx = ctx
}

func (r *Runner) Close() {
	if r == nil {
		return
	}
	if closer, ok := r.traceSink.(interface{ Close() }); ok {
		closer.Close()
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

func (r *Runner) RunBlueprint(appID int, resolutionLogicJSON []byte, inputs map[string]string, opts ...RunOptions) (*dag.RunState, error) {
	return r.executeResolutionWithInputs(appID, resolutionLogicJSON, inputs, firstRunOptions(opts...))
}

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

	baseCtx := opts.Context
	if baseCtx == nil {
		r.mu.Lock()
		baseCtx = r.baseCtx
		r.mu.Unlock()
	}
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	ctx, cancel := context.WithTimeout(baseCtx, 7*24*time.Hour)
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
