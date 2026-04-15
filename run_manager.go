package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

type RunExecutor interface {
	RunBlueprint(appID int, resolutionLogicJSON []byte, inputs map[string]string, opts ...RunOptions) (*dag.RunState, error)
	WriteEvidence(appID int, payload interface{}) (string, error)
}

type activeRun struct {
	request   RunRequest
	result    RunResult
	cancel    context.CancelFunc
	startedAt time.Time
	updatedAt time.Time
	terminal  bool
}

type RunManager struct {
	runner          RunExecutor
	client          *http.Client
	callbackToken   string
	ttl             time.Duration
	cleanupInterval time.Duration
	mu              sync.Mutex
	active          map[string]*activeRun
	byAppID         map[int]string
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
}

type duplicateRunError struct {
	RunID string
}

func (e *duplicateRunError) Error() string {
	return fmt.Sprintf("run already active: %s", e.RunID)
}

func NewRunManager(runner RunExecutor, client *http.Client, callbackToken string) *RunManager {
	return NewRunManagerWithConfig(runner, client, callbackToken, DefaultRunTTL, DefaultRunCleanupInterval)
}

func NewRunManagerWithConfig(runner RunExecutor, client *http.Client, callbackToken string, ttl, cleanupInterval time.Duration) *RunManager {
	ctx, cancel := context.WithCancel(context.Background())
	if client == nil {
		client = &http.Client{Timeout: DefaultHTTPClientTimeout}
	}
	if ttl <= 0 {
		ttl = DefaultRunTTL
	}
	if cleanupInterval <= 0 {
		cleanupInterval = DefaultRunCleanupInterval
	}
	m := &RunManager{
		runner:          runner,
		client:          client,
		callbackToken:   strings.TrimSpace(callbackToken),
		ttl:             ttl,
		cleanupInterval: cleanupInterval,
		active:          make(map[string]*activeRun),
		byAppID:         make(map[int]string),
		ctx:             ctx,
		cancel:          cancel,
	}
	m.wg.Add(1)
	go m.cleanupLoop()
	return m
}

func (m *RunManager) Close() {
	if m == nil {
		return
	}
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
}

func (m *RunManager) Submit(req RunRequest) (RunResult, error) {
	if m == nil {
		return RunResult{}, errors.New("run manager is required")
	}
	if strings.TrimSpace(req.RunID) == "" {
		return RunResult{}, errors.New("run_id is required")
	}
	if req.AppID <= 0 {
		return RunResult{}, errors.New("app_id must be positive")
	}
	if len(req.BlueprintJSON) == 0 {
		return RunResult{}, errors.New("blueprint_json is required")
	}

	m.mu.Lock()
	if existingRunID, ok := m.byAppID[req.AppID]; ok {
		if existing, exists := m.active[existingRunID]; exists && !existing.terminal {
			m.mu.Unlock()
			return RunResult{}, &duplicateRunError{RunID: existingRunID}
		}
	}

	runCtx, cancel := context.WithCancel(m.ctx)
	accepted := RunResult{
		RunID:         req.RunID,
		AppID:         req.AppID,
		BlueprintPath: strings.TrimSpace(req.BlueprintPath),
		Status:        RunStatusAccepted,
		Action:        RunActionNone,
	}
	m.active[req.RunID] = &activeRun{
		request:   req,
		result:    accepted,
		cancel:    cancel,
		startedAt: time.Now().UTC(),
		updatedAt: time.Now().UTC(),
	}
	m.byAppID[req.AppID] = req.RunID
	m.mu.Unlock()

	m.wg.Add(1)
	go m.execute(runCtx, req)

	return accepted, nil
}

func (m *RunManager) execute(ctx context.Context, req RunRequest) {
	defer m.wg.Done()
	m.setRunning(req.RunID)

	run, err := m.runner.RunBlueprint(req.AppID, req.BlueprintJSON, req.Inputs, RunOptions{
		Trace: &TraceMetadata{
			AppID:         req.AppID,
			BlueprintPath: strings.TrimSpace(req.BlueprintPath),
			Initiator:     strings.TrimSpace(req.Initiator),
		},
		Context: ctx,
	})
	if run != nil {
		_, _ = m.runner.WriteEvidence(req.AppID, run)
	}

	result := buildRunResult(req, run, err)
	if validationErr := validateTerminalRunResult(result); validationErr != nil {
		result.Status = RunStatusFailed
		if result.Error == "" {
			result.Error = validationErr.Error()
		} else {
			result.Error = result.Error + "; " + validationErr.Error()
		}
		result.Action = RunActionNone
	}

	m.setTerminal(req.RunID, result)
	if strings.TrimSpace(req.CallbackURL) != "" {
		if err := m.postCallback(req.CallbackURL, result); err != nil {
			slog.Warn("callback delivery failed",
				"component", "run_manager",
				"run_id", req.RunID,
				"callback_url", req.CallbackURL,
				"error", err,
			)
		}
	}
}

func (m *RunManager) setRunning(runID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if run, ok := m.active[runID]; ok {
		run.result.Status = RunStatusRunning
		run.updatedAt = time.Now().UTC()
	}
}

func (m *RunManager) setTerminal(runID string, result RunResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if run, ok := m.active[runID]; ok {
		run.result = result
		run.updatedAt = time.Now().UTC()
		run.terminal = true
		delete(m.byAppID, run.request.AppID)
	}
}

func (m *RunManager) Get(runID string) (RunResult, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	run, ok := m.active[runID]
	if !ok {
		return RunResult{}, false
	}
	return cloneRunResult(run.result), true
}

func (m *RunManager) Cancel(runID string) (RunResult, bool) {
	m.mu.Lock()
	run, ok := m.active[runID]
	if !ok {
		m.mu.Unlock()
		return RunResult{}, false
	}
	result := cloneRunResult(run.result)
	terminal := run.terminal
	cancel := run.cancel
	m.mu.Unlock()

	if !terminal && cancel != nil {
		cancel()
	}
	return result, true
}

func (m *RunManager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, run := range m.active {
		if !run.terminal {
			count++
		}
	}
	return count
}

func (m *RunManager) cleanupLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.cleanupExpired()
		}
	}
}

func (m *RunManager) cleanupExpired() {
	cutoff := time.Now().UTC().Add(-m.ttl)
	m.mu.Lock()
	defer m.mu.Unlock()
	for runID, run := range m.active {
		if run.terminal && run.updatedAt.Before(cutoff) {
			delete(m.active, runID)
		}
	}
}

func (m *RunManager) postCallback(callbackURL string, result RunResult) error {
	payload, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal callback result: %w", err)
	}
	req, err := http.NewRequestWithContext(m.ctx, http.MethodPost, callbackURL, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build callback request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if m.callbackToken != "" {
		req.Header.Set("Authorization", "Bearer "+m.callbackToken)
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return fmt.Errorf("post callback result: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("callback returned status %d", resp.StatusCode)
	}
	return nil
}

func cloneRunResult(result RunResult) RunResult {
	cloned := result
	if result.RunState != nil {
		cloned.RunState = cloneDAGRunState(result.RunState)
	}
	return cloned
}

func cloneDAGRunState(run *dag.RunState) *dag.RunState {
	if run == nil {
		return nil
	}
	cloned := *run
	cloned.Definition = cloneBlueprint(run.Definition)
	cloned.Inputs = cloneStringMap(run.Inputs)
	cloned.NodeStates = cloneNodeStateMap(run.NodeStates)
	cloned.Context = cloneStringMap(run.Context)
	cloned.EdgeTraversals = cloneIntMap(run.EdgeTraversals)
	cloned.NodeTraces = cloneNodeTraces(run.NodeTraces)
	return &cloned
}

func cloneBlueprint(bp dag.Blueprint) dag.Blueprint {
	cloned := bp
	if bp.Nodes != nil {
		cloned.Nodes = append([]dag.NodeDef(nil), bp.Nodes...)
		for i := range cloned.Nodes {
			cloned.Nodes[i].Config = cloneJSONValue(cloned.Nodes[i].Config)
		}
	}
	if bp.Edges != nil {
		cloned.Edges = append([]dag.EdgeDef(nil), bp.Edges...)
	}
	if bp.Inputs != nil {
		cloned.Inputs = append([]dag.InputDef(nil), bp.Inputs...)
	}
	if bp.Budget != nil {
		budget := *bp.Budget
		if bp.Budget.PerNode != nil {
			budget.PerNode = make(map[string]dag.NodeBud, len(bp.Budget.PerNode))
			for key, value := range bp.Budget.PerNode {
				budget.PerNode[key] = value
			}
		}
		cloned.Budget = &budget
	}
	return cloned
}

func cloneJSONValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var cloned interface{}
	if err := json.Unmarshal(data, &cloned); err != nil {
		return value
	}
	return cloned
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneIntMap(values map[string]int) map[string]int {
	if values == nil {
		return nil
	}
	cloned := make(map[string]int, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneNodeStateMap(values map[string]dag.NodeState) map[string]dag.NodeState {
	if values == nil {
		return nil
	}
	cloned := make(map[string]dag.NodeState, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneNodeTraces(values []dag.NodeTrace) []dag.NodeTrace {
	if values == nil {
		return nil
	}
	cloned := make([]dag.NodeTrace, len(values))
	for i, trace := range values {
		cloned[i] = trace
		cloned[i].InputSnapshot = cloneStringMap(trace.InputSnapshot)
		cloned[i].Outputs = cloneStringMap(trace.Outputs)
	}
	return cloned
}
