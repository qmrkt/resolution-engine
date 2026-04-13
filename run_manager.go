package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	return NewRunManagerWithConfig(runner, client, callbackToken, 30*time.Minute, time.Minute)
}

func NewRunManagerWithConfig(runner RunExecutor, client *http.Client, callbackToken string, ttl, cleanupInterval time.Duration) *RunManager {
	ctx, cancel := context.WithCancel(context.Background())
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	if cleanupInterval <= 0 {
		cleanupInterval = time.Minute
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
		_ = m.postCallback(req.CallbackURL, result)
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
		cloned.RunState = result.RunState
	}
	return cloned
}
