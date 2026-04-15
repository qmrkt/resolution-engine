package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

type DurableRunManagerConfig struct {
	MaxWorkers   int
	MaxQueueSize int
	PollInterval time.Duration
}

type DurableRunManager struct {
	runner        *Runner
	store         *durableFileStore
	client        *http.Client
	callbackToken string
	maxWorkers    int
	maxQueueSize  int
	pollInterval  time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	queue  chan string

	mu         sync.Mutex
	processing map[string]bool
	runCancels map[string]context.CancelFunc
}

type queueFullError struct {
	MaxQueueSize int
}

func (e *queueFullError) Error() string {
	return fmt.Sprintf("workflow queue full: max queued runs %d", e.MaxQueueSize)
}

func NewDurableRunManager(runner *Runner, dataDir string, client *http.Client, callbackToken string, cfg DurableRunManagerConfig) (*DurableRunManager, error) {
	if runner == nil {
		return nil, errors.New("runner is required")
	}
	store, err := newDurableFileStore(dataDir)
	if err != nil {
		return nil, err
	}
	if client == nil {
		client = &http.Client{Timeout: DefaultHTTPClientTimeout}
	}
	if cfg.MaxWorkers <= 0 {
		cfg.MaxWorkers = DefaultMaxWorkers
	}
	if cfg.MaxQueueSize <= 0 {
		cfg.MaxQueueSize = DefaultMaxQueueSize
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = DefaultPollInterval
	}
	ctx, cancel := context.WithCancel(context.Background())
	manager := &DurableRunManager{
		runner:        runner,
		store:         store,
		client:        client,
		callbackToken: strings.TrimSpace(callbackToken),
		maxWorkers:    cfg.MaxWorkers,
		maxQueueSize:  cfg.MaxQueueSize,
		pollInterval:  cfg.PollInterval,
		ctx:           ctx,
		cancel:        cancel,
		queue:         make(chan string, cfg.MaxQueueSize),
		processing:    make(map[string]bool),
		runCancels:    make(map[string]context.CancelFunc),
	}
	if err := manager.recoverRuns(); err != nil {
		cancel()
		return nil, err
	}
	for i := 0; i < manager.maxWorkers; i++ {
		manager.wg.Add(1)
		go manager.worker()
	}
	manager.wg.Add(2)
	go manager.timerLoop()
	go manager.outboxLoop()
	return manager, nil
}

func (m *DurableRunManager) Close() {
	if m == nil {
		return
	}
	m.cancel()
	m.mu.Lock()
	for _, cancel := range m.runCancels {
		cancel()
	}
	m.mu.Unlock()
	m.wg.Wait()
}

func (m *DurableRunManager) Submit(req RunRequest) (RunResult, error) {
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

	var bp dag.Blueprint
	if err := json.Unmarshal(req.BlueprintJSON, &bp); err != nil {
		return RunResult{}, fmt.Errorf("blueprint_json must be valid json: %w", err)
	}
	if errs := dag.ValidateBlueprint(bp); len(errs) > 0 {
		return RunResult{}, fmt.Errorf("blueprint validation failed: %v", errs[0])
	}
	for _, node := range bp.Nodes {
		if _, ok := m.runner.engine.ExecutorFor(node.Type); !ok && !isDurableSuspendNode(node.Type) {
			return RunResult{}, fmt.Errorf("no executor registered for node type %q", node.Type)
		}
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	inputs := map[string]string{"market_app_id": strconv.Itoa(req.AppID)}
	for key, value := range req.Inputs {
		if strings.TrimSpace(key) == "" {
			continue
		}
		inputs[key] = value
	}
	execCtx := dag.NewContext(inputs)
	execCtx.Set("resolution_run_id", req.RunID)
	execCtx.Set("input.resolution_run_id", req.RunID)
	execCtx.Set("__run_id", req.RunID)
	scheduler := dag.NewScheduler(bp)
	run := &dag.RunState{
		ID:             req.RunID,
		BlueprintID:    bp.ID,
		Definition:     bp,
		Status:         string(RunStatusQueued),
		Inputs:         execCtx.Snapshot(),
		NodeStates:     make(map[string]dag.NodeState, len(bp.Nodes)),
		Context:        execCtx.Snapshot(),
		EdgeTraversals: make(map[string]int),
		StartedAt:      now,
	}
	for _, node := range bp.Nodes {
		run.NodeStates[node.ID] = dag.NodeState{Status: "pending"}
	}
	record := &durableRunRecord{
		Request: req,
		Result: RunResult{
			RunID:         req.RunID,
			AppID:         req.AppID,
			BlueprintPath: strings.TrimSpace(req.BlueprintPath),
			Status:        RunStatusQueued,
			Action:        RunActionNone,
			RunState:      run,
		},
		Checkpoint: durableCheckpoint{
			Run:            run,
			Context:        execCtx.Snapshot(),
			Completed:      make(map[string]bool),
			Failed:         make(map[string]bool),
			Activated:      boolMapFromSet(scheduler.InitialActivatedNodes()),
			Skipped:        make(map[string]bool),
			IterationCount: make(map[string]int),
			EdgeTraversals: make(map[string]int),
			Waiting:        make(map[string]durableWaitingNode),
		},
		Signals:   make(map[string]signalRequest),
		CreatedAt: now,
		UpdatedAt: now,
	}

	m.mu.Lock()
	if existing, ok, err := m.activeRunForApp(req.AppID); err != nil {
		m.mu.Unlock()
		return RunResult{}, err
	} else if ok {
		m.mu.Unlock()
		return RunResult{}, &duplicateRunError{RunID: existing.Request.RunID}
	}
	if m.maxQueueSize > 0 {
		queued, err := m.queuedRunCount()
		if err != nil {
			m.mu.Unlock()
			return RunResult{}, err
		}
		if queued >= m.maxQueueSize {
			m.mu.Unlock()
			return RunResult{}, &queueFullError{MaxQueueSize: m.maxQueueSize}
		}
	}
	if err := m.store.saveRun(record); err != nil {
		m.mu.Unlock()
		return RunResult{}, err
	}
	m.mu.Unlock()
	_ = m.store.appendEvent(req.RunID, "RunQueued", map[string]interface{}{"app_id": req.AppID})
	m.enqueue(req.RunID)
	return cloneRunResult(record.Result), nil
}

func (m *DurableRunManager) Get(runID string) (RunResult, bool) {
	record, err := m.store.loadRun(runID)
	if err != nil {
		return RunResult{}, false
	}
	return cloneRunResult(record.Result), true
}

func (m *DurableRunManager) Cancel(runID string) (RunResult, bool) {
	m.mu.Lock()
	record, err := m.store.loadRun(runID)
	if err != nil {
		m.mu.Unlock()
		return RunResult{}, false
	}
	if isTerminalStatus(record.Result.Status) {
		m.mu.Unlock()
		return cloneRunResult(record.Result), true
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	record.Result.Status = RunStatusCancelled
	record.Result.Action = RunActionNone
	record.Result.Error = "run cancelled"
	record.CompletedAt = now
	record.Checkpoint.Waiting = make(map[string]durableWaitingNode)
	if record.Checkpoint.Run != nil {
		record.Checkpoint.Run.Status = string(RunStatusCancelled)
		record.Checkpoint.Run.Error = "run cancelled"
		record.Checkpoint.Run.CompletedAt = now
		record.Result.RunState = record.Checkpoint.Run
	}
	if err := m.store.saveRun(record); err != nil {
		m.mu.Unlock()
		return RunResult{}, false
	}
	if cancel := m.runCancels[runID]; cancel != nil {
		cancel()
	}
	m.mu.Unlock()
	_ = m.store.appendEvent(runID, "RunCancelled", nil)
	return cloneRunResult(record.Result), true
}

func (m *DurableRunManager) ActiveCount() int {
	runs, err := m.store.listRuns()
	if err != nil {
		return 0
	}
	count := 0
	for _, run := range runs {
		if !isTerminalStatus(run.Result.Status) {
			count++
		}
	}
	return count
}

func (m *DurableRunManager) Signal(req signalRequest) (signalResult, error) {
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		return signalResult{}, errors.New("idempotency_key is required")
	}
	if strings.TrimSpace(req.SignalType) == "" {
		return signalResult{}, errors.New("signal_type is required")
	}
	if req.Payload == nil {
		req.Payload = map[string]string{}
	}
	if req.ObservedAt == "" {
		req.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}

	m.mu.Lock()
	record, err := m.findSignalTarget(req)
	if err != nil {
		m.mu.Unlock()
		return signalResult{}, err
	}
	if record == nil {
		m.mu.Unlock()
		return signalResult{}, errors.New("no matching run for signal")
	}
	ensureDurableRecordMaps(record)
	if isTerminalStatus(record.Result.Status) {
		result := signalResult{RunID: record.Request.RunID, AppID: record.Request.AppID, Status: record.Result.Status}
		m.mu.Unlock()
		return result, nil
	}
	if _, exists := record.Signals[req.IdempotencyKey]; exists {
		result := signalResult{RunID: record.Request.RunID, AppID: record.Request.AppID, Status: record.Result.Status}
		m.mu.Unlock()
		return result, nil
	}

	record.Signals[req.IdempotencyKey] = req
	matched := false
	for nodeID, waiting := range record.Checkpoint.Waiting {
		if waitingMatchesSignal(waiting, req) {
			if err := validateSignalForWaiting(waiting, req); err != nil {
				m.mu.Unlock()
				return signalResult{}, err
			}
			if err := m.completeWaitingNode(record, nodeID, outputsForSignal(waiting, req)); err != nil {
				m.mu.Unlock()
				return signalResult{}, err
			}
			matched = true
		}
	}
	if matched && !isTerminalStatus(record.Result.Status) {
		record.Result.Status = RunStatusQueued
		if record.Checkpoint.Run != nil {
			record.Checkpoint.Run.Status = string(RunStatusQueued)
			record.Result.RunState = record.Checkpoint.Run
		}
	}
	if err := m.store.saveRun(record); err != nil {
		m.mu.Unlock()
		return signalResult{}, err
	}
	result := signalResult{RunID: record.Request.RunID, AppID: record.Request.AppID, Status: record.Result.Status}
	runID := record.Request.RunID
	m.mu.Unlock()
	_ = m.store.appendEvent(runID, "SignalReceived", req)
	if matched {
		m.enqueue(runID)
	}
	return result, nil
}

func (m *DurableRunManager) worker() {
	defer m.wg.Done()
	for {
		select {
		case <-m.ctx.Done():
			return
		case runID := <-m.queue:
			m.processRunOnce(runID)
		}
	}
}

func (m *DurableRunManager) processRunOnce(runID string) {
	if !m.markProcessing(runID) {
		return
	}
	defer m.unmarkProcessing(runID)

	runCtx, cancel := context.WithCancel(m.ctx)
	m.mu.Lock()
	m.runCancels[runID] = cancel
	m.mu.Unlock()
	defer func() {
		cancel()
		m.mu.Lock()
		delete(m.runCancels, runID)
		m.mu.Unlock()
	}()

	if err := m.advanceRun(runCtx, runID); err != nil && !errors.Is(err, context.Canceled) {
		record, loadErr := m.store.loadRun(runID)
		if loadErr != nil || isTerminalStatus(record.Result.Status) {
			return
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		record.Result.Status = RunStatusFailed
		record.Result.Error = err.Error()
		record.CompletedAt = now
		if record.Checkpoint.Run != nil {
			record.Checkpoint.Run.Status = "failed"
			record.Checkpoint.Run.Error = err.Error()
			record.Checkpoint.Run.CompletedAt = now
			record.Result.RunState = record.Checkpoint.Run
		}
		_ = m.store.saveRun(record)
		_ = m.store.appendEvent(runID, "RunFailed", map[string]string{"error": err.Error()})
	}
}

func (m *DurableRunManager) advanceRun(ctx context.Context, runID string) error {
	record, err := m.store.loadRun(runID)
	if err != nil {
		return err
	}
	ensureDurableRecordMaps(record)
	if isTerminalStatus(record.Result.Status) {
		return nil
	}
	if record.Result.Status == RunStatusWaiting && !hasDueWaiting(record.Checkpoint.Waiting, time.Now().Unix()) {
		return nil
	}

	record.Result.Status = RunStatusRunning
	if record.Checkpoint.Run != nil {
		record.Checkpoint.Run.Status = string(RunStatusRunning)
		record.Result.RunState = record.Checkpoint.Run
	}
	if err := m.store.saveRun(record); err != nil {
		return err
	}
	_ = m.store.appendEvent(runID, "RunRunning", nil)

	for {
		if err := ctx.Err(); err != nil {
			return m.handleInterruptedRun(runID, err)
		}
		if err := m.applyDueWaiting(record, time.Now().Unix()); err != nil {
			return err
		}
		if isTerminalStatus(record.Result.Status) {
			return m.store.saveRun(record)
		}

		ready := m.readyNodes(record)
		if len(ready) == 0 {
			if len(record.Checkpoint.Waiting) > 0 {
				record.Result.Status = RunStatusWaiting
				if record.Checkpoint.Run != nil {
					record.Checkpoint.Run.Status = string(RunStatusWaiting)
					record.Result.RunState = record.Checkpoint.Run
				}
				if err := m.store.saveRun(record); err != nil {
					return err
				}
				_ = m.store.appendEvent(runID, "RunWaiting", map[string]interface{}{"waiting": record.Checkpoint.Waiting})
				return nil
			}
			return m.completeRun(record)
		}

		nodeID := ready[0]
		if err := m.executeNode(ctx, record, nodeID); err != nil {
			return err
		}
		if err := m.store.saveRun(record); err != nil {
			return err
		}
		if len(record.Checkpoint.Waiting) > 0 {
			record.Result.Status = RunStatusWaiting
			if record.Checkpoint.Run != nil {
				record.Checkpoint.Run.Status = string(RunStatusWaiting)
				record.Result.RunState = record.Checkpoint.Run
			}
			if err := m.store.saveRun(record); err != nil {
				return err
			}
			_ = m.store.appendEvent(runID, "RunWaiting", map[string]interface{}{"waiting": record.Checkpoint.Waiting})
			return nil
		}
	}
}

func (m *DurableRunManager) executeNode(ctx context.Context, record *durableRunRecord, nodeID string) error {
	node, ok := nodeByID(record.Checkpoint.Run.Definition, nodeID)
	if !ok {
		return fmt.Errorf("node %q not found", nodeID)
	}
	execCtx := dag.NewContextFromSnapshot(record.Checkpoint.Context)
	record.Checkpoint.IterationCount[nodeID]++
	iteration := record.Checkpoint.IterationCount[nodeID]
	startedAt := time.Now().UTC().Format(time.RFC3339Nano)
	inputSnapshot := execCtx.Snapshot()
	state := record.Checkpoint.Run.NodeStates[nodeID]
	state.Status = "running"
	state.StartedAt = startedAt
	state.CompletedAt = ""
	state.Error = ""
	state.Usage = dag.TokenUsage{}
	record.Checkpoint.Run.NodeStates[nodeID] = state
	record.Result.RunState = record.Checkpoint.Run
	if err := m.store.saveRun(record); err != nil {
		return err
	}
	_ = m.store.appendEvent(record.Request.RunID, "NodeStarted", map[string]interface{}{"node_id": nodeID, "iteration": iteration})

	if suspended, err := m.trySuspendNode(record, node, execCtx, durableWaitingNode{
		NodeID:        nodeID,
		StartedAt:     startedAt,
		InputSnapshot: inputSnapshot,
		Iteration:     iteration,
	}); suspended || err != nil {
		return err
	}

	exec, ok := m.runner.engine.ExecutorFor(node.Type)
	if !ok {
		return fmt.Errorf("no executor registered for node type %q", node.Type)
	}
	result, err := exec.Execute(ctx, node, execCtx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		m.failNode(record, node, err, startedAt, inputSnapshot, iteration)
		return nil
	}
	return m.completeNode(record, node, result.Outputs, result.Usage, startedAt, inputSnapshot, iteration)
}

func (m *DurableRunManager) trySuspendNode(record *durableRunRecord, node dag.NodeDef, execCtx *dag.Context, waiting durableWaitingNode) (bool, error) {
	switch node.Type {
	case "wait":
		outputs, waitUntil, done, err := durableWaitDecision(node, execCtx)
		if err != nil {
			m.failNode(record, node, err, waiting.StartedAt, waiting.InputSnapshot, waiting.Iteration)
			return false, nil
		}
		if done {
			return true, m.completeNode(record, node, outputs, dag.TokenUsage{}, waiting.StartedAt, waiting.InputSnapshot, waiting.Iteration)
		}
		waiting.Kind = "timer"
		waiting.Reason = "timer"
		waiting.ResumeAtUnix = waitUntil
		waiting.Outputs = outputs
	case "human_judge":
		cfg, err := decodeNodeConfig[durableHumanJudgeConfig](node.Config)
		if err != nil {
			m.failNode(record, node, err, waiting.StartedAt, waiting.InputSnapshot, waiting.Iteration)
			return false, nil
		}
		timeoutSeconds := cfg.TimeoutSeconds
		if timeoutSeconds <= 0 {
			timeoutSeconds = DefaultDurableHumanJudgeTimeout
		}
		waiting.Kind = "signal"
		waiting.Reason = "human judgment"
		waiting.SignalType = "human_judgment"
		waiting.CorrelationKey = fmt.Sprintf("%d:%s:%s", record.Request.AppID, record.Request.RunID, node.ID)
		waiting.ResumeAtUnix = time.Now().Add(time.Duration(timeoutSeconds) * time.Second).Unix()
		waiting.TimeoutOutputs = map[string]string{"status": "timeout"}
	case "defer_resolution":
		cfg, _ := decodeNodeConfig[durableDeferResolutionConfig](node.Config)
		reason := strings.TrimSpace(cfg.Reason)
		if reason == "" {
			reason = "resolution deferred"
		}
		waiting.Kind = "signal"
		waiting.Reason = reason
		waiting.SignalType = strings.TrimSpace(cfg.ResumeSignal)
		if waiting.SignalType == "" {
			waiting.SignalType = "defer_resolution.resume"
		}
		waiting.CorrelationKey = strings.TrimSpace(cfg.CorrelationKey)
		if waiting.CorrelationKey == "" {
			waiting.CorrelationKey = fmt.Sprintf("%d:%s:%s", record.Request.AppID, record.Request.RunID, node.ID)
		}
	default:
		return false, nil
	}

	for _, signal := range sortedSignals(record.Signals) {
		if waitingMatchesSignal(waiting, signal) {
			return true, m.completeNode(record, node, outputsForSignal(waiting, signal), dag.TokenUsage{}, waiting.StartedAt, waiting.InputSnapshot, waiting.Iteration)
		}
	}

	state := record.Checkpoint.Run.NodeStates[node.ID]
	state.Status = "waiting"
	record.Checkpoint.Run.NodeStates[node.ID] = state
	record.Checkpoint.Waiting[node.ID] = waiting
	record.Result.RunState = record.Checkpoint.Run
	_ = m.store.appendEvent(record.Request.RunID, "NodeSuspended", waiting)
	return true, nil
}

func (m *DurableRunManager) completeWaitingNode(record *durableRunRecord, nodeID string, outputs map[string]string) error {
	waiting, ok := record.Checkpoint.Waiting[nodeID]
	if !ok {
		return nil
	}
	node, ok := nodeByID(record.Checkpoint.Run.Definition, nodeID)
	if !ok {
		return fmt.Errorf("node %q not found", nodeID)
	}
	return m.completeNode(record, node, outputs, dag.TokenUsage{}, waiting.StartedAt, waiting.InputSnapshot, waiting.Iteration)
}

func (m *DurableRunManager) completeNode(record *durableRunRecord, node dag.NodeDef, outputs map[string]string, usage dag.TokenUsage, startedAt string, inputSnapshot map[string]string, iteration int) error {
	if outputs == nil {
		outputs = map[string]string{}
	}
	execCtx := dag.NewContextFromSnapshot(record.Checkpoint.Context)
	for key, value := range outputs {
		execCtx.Set(node.ID+"."+key, value)
	}
	if execCtx.Get(node.ID+".status") == "" {
		execCtx.Set(node.ID+".status", "completed")
	}

	completedAt := time.Now().UTC().Format(time.RFC3339Nano)
	state := record.Checkpoint.Run.NodeStates[node.ID]
	state.Status = "completed"
	state.CompletedAt = completedAt
	state.Error = ""
	state.Usage = usage
	record.Checkpoint.Run.NodeStates[node.ID] = state
	record.Checkpoint.Run.NodeTraces = append(record.Checkpoint.Run.NodeTraces, dag.NodeTrace{
		NodeID:        node.ID,
		NodeType:      node.Type,
		NodeLabel:     node.Label,
		Iteration:     iteration,
		Status:        "completed",
		InputSnapshot: cloneStringMap(inputSnapshot),
		Outputs:       cloneStringMap(outputs),
		Usage:         usage,
		StartedAt:     startedAt,
		CompletedAt:   completedAt,
	})
	record.Checkpoint.Run.Usage.InputTokens += usage.InputTokens
	record.Checkpoint.Run.Usage.OutputTokens += usage.OutputTokens
	record.Checkpoint.Completed[node.ID] = true
	delete(record.Checkpoint.Failed, node.ID)
	delete(record.Checkpoint.Waiting, node.ID)
	record.Checkpoint.Context = execCtx.Snapshot()
	record.Checkpoint.Run.Context = execCtx.Snapshot()
	if err := m.evaluateOutgoingEdges(record, node.ID, execCtx); err != nil {
		return err
	}
	record.Result.RunState = record.Checkpoint.Run
	_ = m.store.appendEvent(record.Request.RunID, "NodeCompleted", map[string]interface{}{"node_id": node.ID, "outputs": outputs})
	return nil
}

func (m *DurableRunManager) failNode(record *durableRunRecord, node dag.NodeDef, err error, startedAt string, inputSnapshot map[string]string, iteration int) {
	execCtx := dag.NewContextFromSnapshot(record.Checkpoint.Context)
	execCtx.Set(node.ID+".status", "failed")
	execCtx.Set(node.ID+".error", err.Error())
	completedAt := time.Now().UTC().Format(time.RFC3339Nano)
	state := record.Checkpoint.Run.NodeStates[node.ID]
	state.Status = "failed"
	state.CompletedAt = completedAt
	state.Error = err.Error()
	record.Checkpoint.Run.NodeStates[node.ID] = state
	record.Checkpoint.Run.NodeTraces = append(record.Checkpoint.Run.NodeTraces, dag.NodeTrace{
		NodeID:        node.ID,
		NodeType:      node.Type,
		NodeLabel:     node.Label,
		Iteration:     iteration,
		Status:        "failed",
		InputSnapshot: cloneStringMap(inputSnapshot),
		Error:         err.Error(),
		StartedAt:     startedAt,
		CompletedAt:   completedAt,
	})
	record.Checkpoint.Context = execCtx.Snapshot()
	record.Checkpoint.Run.Context = execCtx.Snapshot()
	if node.OnError == "continue" {
		record.Checkpoint.Completed[node.ID] = true
	} else {
		record.Checkpoint.Failed[node.ID] = true
	}
	record.Result.RunState = record.Checkpoint.Run
	_ = m.store.appendEvent(record.Request.RunID, "NodeFailed", map[string]interface{}{"node_id": node.ID, "error": err.Error()})
}

func (m *DurableRunManager) evaluateOutgoingEdges(record *durableRunRecord, nodeID string, execCtx *dag.Context) error {
	scheduler := dag.NewScheduler(record.Checkpoint.Run.Definition)
	scheduler.RestoreTraversals(record.Checkpoint.EdgeTraversals)
	edges, err := scheduler.EvaluateEdges(nodeID, execCtx)
	if err != nil {
		return err
	}
	for _, edge := range edges {
		scheduler.TrackTraversal(edge.From, edge.To)
		record.Checkpoint.Activated[edge.To] = true
		if record.Checkpoint.Completed[edge.To] {
			execCtx.AppendNodeHistory(edge.To)
			delete(record.Checkpoint.Completed, edge.To)
			state := record.Checkpoint.Run.NodeStates[edge.To]
			state.Status = "pending"
			state.CompletedAt = ""
			state.Error = ""
			state.Usage = dag.TokenUsage{}
			record.Checkpoint.Run.NodeStates[edge.To] = state
		}
	}
	record.Checkpoint.EdgeTraversals = scheduler.TraversalSnapshot()
	record.Checkpoint.Run.EdgeTraversals = cloneIntMap(record.Checkpoint.EdgeTraversals)
	record.Checkpoint.Context = execCtx.Snapshot()
	record.Checkpoint.Run.Context = execCtx.Snapshot()
	return nil
}

func (m *DurableRunManager) completeRun(record *durableRunRecord) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	record.Checkpoint.Run.Context = cloneStringMap(record.Checkpoint.Context)
	record.Checkpoint.Run.EdgeTraversals = cloneIntMap(record.Checkpoint.EdgeTraversals)
	record.Checkpoint.Run.CompletedAt = now
	if len(record.Checkpoint.Failed) > 0 {
		record.Checkpoint.Run.Status = "failed"
		if record.Checkpoint.Run.Error == "" {
			record.Checkpoint.Run.Error = "one or more nodes failed"
		}
		record.Result = buildRunResult(record.Request, record.Checkpoint.Run, errors.New(record.Checkpoint.Run.Error))
	} else {
		record.Checkpoint.Run.Status = "completed"
		record.Checkpoint.Run.Error = ""
		record.Result = buildRunResult(record.Request, record.Checkpoint.Run, nil)
		if validationErr := validateTerminalRunResult(record.Result); validationErr != nil {
			record.Result.Status = RunStatusFailed
			record.Result.Error = validationErr.Error()
			record.Checkpoint.Run.Status = "failed"
			record.Checkpoint.Run.Error = validationErr.Error()
		}
	}
	record.Result.RunState = record.Checkpoint.Run
	record.CompletedAt = now
	_, _ = m.runner.WriteEvidence(record.Request.AppID, record.Checkpoint.Run)
	if err := m.store.saveRun(record); err != nil {
		return err
	}
	_ = m.store.appendEvent(record.Request.RunID, "RunCompleted", map[string]interface{}{"status": record.Result.Status, "action": record.Result.Action})
	return nil
}

func (m *DurableRunManager) readyNodes(record *durableRunRecord) []string {
	scheduler := dag.NewScheduler(record.Checkpoint.Run.Definition)
	scheduler.RestoreTraversals(record.Checkpoint.EdgeTraversals)
	for nodeID := range record.Checkpoint.Skipped {
		scheduler.Skipped[nodeID] = struct{}{}
	}
	ready := scheduler.ReadyNodes(setFromBoolMap(record.Checkpoint.Completed), setFromBoolMap(record.Checkpoint.Failed), nil)
	for nodeID := range scheduler.Skipped {
		if !record.Checkpoint.Skipped[nodeID] {
			record.Checkpoint.Skipped[nodeID] = true
			state := record.Checkpoint.Run.NodeStates[nodeID]
			state.Status = "skipped"
			state.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
			record.Checkpoint.Run.NodeStates[nodeID] = state
		}
	}
	filtered := make([]string, 0, len(ready))
	for _, nodeID := range ready {
		if !record.Checkpoint.Activated[nodeID] || record.Checkpoint.Waiting[nodeID].NodeID != "" {
			continue
		}
		filtered = append(filtered, nodeID)
	}
	sort.Strings(filtered)
	return filtered
}

func (m *DurableRunManager) timerLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			_ = m.wakeDueRuns()
		}
	}
}

func (m *DurableRunManager) wakeDueRuns() error {
	runs, err := m.store.listRuns()
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	for _, record := range runs {
		if isTerminalStatus(record.Result.Status) || !hasDueWaiting(record.Checkpoint.Waiting, now) {
			continue
		}
		if err := m.applyDueWaiting(record, now); err != nil {
			return err
		}
		if !isTerminalStatus(record.Result.Status) {
			record.Result.Status = RunStatusQueued
			if record.Checkpoint.Run != nil {
				record.Checkpoint.Run.Status = string(RunStatusQueued)
				record.Result.RunState = record.Checkpoint.Run
			}
		}
		if err := m.store.saveRun(record); err != nil {
			return err
		}
		m.enqueue(record.Request.RunID)
	}
	return nil
}

func (m *DurableRunManager) applyDueWaiting(record *durableRunRecord, now int64) error {
	for nodeID, waiting := range cloneWaitingMap(record.Checkpoint.Waiting) {
		if waiting.ResumeAtUnix <= 0 || waiting.ResumeAtUnix > now {
			continue
		}
		outputs := waiting.Outputs
		if waiting.Kind == "signal" && len(waiting.TimeoutOutputs) > 0 {
			outputs = waiting.TimeoutOutputs
		}
		if outputs == nil {
			continue
		}
		if err := m.completeWaitingNode(record, nodeID, outputs); err != nil {
			return err
		}
		_ = m.store.appendEvent(record.Request.RunID, "TimerFired", map[string]interface{}{"node_id": nodeID})
	}
	return nil
}

func (m *DurableRunManager) outboxLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			_ = m.deliverCallbacks()
		}
	}
}

func (m *DurableRunManager) deliverCallbacks() error {
	runs, err := m.store.listRuns()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, record := range runs {
		if !isTerminalStatus(record.Result.Status) || record.CallbackDelivered || strings.TrimSpace(record.Request.CallbackURL) == "" {
			continue
		}
		if record.NextCallbackAt != "" {
			next, err := time.Parse(time.RFC3339Nano, record.NextCallbackAt)
			if err == nil && next.After(now) {
				continue
			}
		}
		if err := m.postCallback(record.Request.CallbackURL, record.Result); err != nil {
			record.CallbackAttempts++
			delay := time.Duration(record.CallbackAttempts) * time.Second
			if delay > DefaultCallbackMaxBackoff {
				delay = DefaultCallbackMaxBackoff
			}
			record.NextCallbackAt = now.Add(delay).Format(time.RFC3339Nano)
			_ = m.store.saveRun(record)
			_ = m.store.appendEvent(record.Request.RunID, "CallbackFailed", map[string]string{"error": err.Error()})
			continue
		}
		record.CallbackDelivered = true
		record.NextCallbackAt = ""
		if err := m.store.saveRun(record); err != nil {
			return err
		}
		_ = m.store.appendEvent(record.Request.RunID, "CallbackDelivered", nil)
	}
	return nil
}

func (m *DurableRunManager) postCallback(callbackURL string, result RunResult) error {
	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(m.ctx, http.MethodPost, callbackURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if m.callbackToken != "" {
		req.Header.Set("Authorization", "Bearer "+m.callbackToken)
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("callback returned status %d", resp.StatusCode)
	}
	return nil
}

func (m *DurableRunManager) recoverRuns() error {
	runs, err := m.store.listRuns()
	if err != nil {
		return err
	}
	for _, record := range runs {
		if record.Result.Status == RunStatusRunning {
			record.Result.Status = RunStatusQueued
			if record.Checkpoint.Run != nil {
				record.Checkpoint.Run.Status = string(RunStatusQueued)
				record.Result.RunState = record.Checkpoint.Run
			}
			if err := m.store.saveRun(record); err != nil {
				return err
			}
			_ = m.store.appendEvent(record.Request.RunID, "RunRecovered", nil)
		}
		if record.Result.Status == RunStatusQueued {
			m.enqueue(record.Request.RunID)
		}
	}
	return nil
}

func (m *DurableRunManager) activeRunForApp(appID int) (*durableRunRecord, bool, error) {
	runs, err := m.store.listRuns()
	if err != nil {
		return nil, false, err
	}
	for _, run := range runs {
		if run.Request.AppID == appID && !isTerminalStatus(run.Result.Status) {
			return run, true, nil
		}
	}
	return nil, false, nil
}

func (m *DurableRunManager) queuedRunCount() (int, error) {
	runs, err := m.store.listRuns()
	if err != nil {
		return 0, err
	}
	count := 0
	for _, run := range runs {
		if run.Result.Status == RunStatusQueued {
			count++
		}
	}
	return count, nil
}

func (m *DurableRunManager) findSignalTarget(req signalRequest) (*durableRunRecord, error) {
	if strings.TrimSpace(req.RunID) != "" {
		return m.store.loadRun(req.RunID)
	}
	runs, err := m.store.listRuns()
	if err != nil {
		return nil, err
	}
	for _, run := range runs {
		if req.AppID > 0 && run.Request.AppID != req.AppID {
			continue
		}
		for _, waiting := range run.Checkpoint.Waiting {
			if waitingMatchesSignal(waiting, req) {
				return run, nil
			}
		}
	}
	return nil, nil
}

func (m *DurableRunManager) handleInterruptedRun(runID string, err error) error {
	record, loadErr := m.store.loadRun(runID)
	if loadErr != nil {
		return err
	}
	if isTerminalStatus(record.Result.Status) {
		return nil
	}
	record.Result.Status = RunStatusQueued
	if record.Checkpoint.Run != nil {
		record.Checkpoint.Run.Status = string(RunStatusQueued)
		record.Result.RunState = record.Checkpoint.Run
	}
	_ = m.store.saveRun(record)
	_ = m.store.appendEvent(runID, "RunInterrupted", map[string]string{"error": err.Error()})
	return err
}

func (m *DurableRunManager) enqueue(runID string) {
	select {
	case m.queue <- runID:
	case <-m.ctx.Done():
	default:
		go func() {
			select {
			case m.queue <- runID:
			case <-m.ctx.Done():
			}
		}()
	}
}

func (m *DurableRunManager) markProcessing(runID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.processing[runID] {
		return false
	}
	m.processing[runID] = true
	return true
}

func (m *DurableRunManager) unmarkProcessing(runID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.processing, runID)
}

type durableWaitConfig struct {
	DurationSeconds int    `json:"duration_seconds"`
	Mode            string `json:"mode,omitempty"`
	StartFrom       string `json:"start_from,omitempty"`
}

type durableHumanJudgeConfig struct {
	TimeoutSeconds int `json:"timeout_seconds"`
}

type durableDeferResolutionConfig struct {
	Reason         string `json:"reason,omitempty"`
	ResumeSignal   string `json:"resume_signal,omitempty"`
	CorrelationKey string `json:"correlation_key,omitempty"`
}

func durableWaitDecision(node dag.NodeDef, execCtx *dag.Context) (map[string]string, int64, bool, error) {
	cfg, err := decodeNodeConfig[durableWaitConfig](node.Config)
	if err != nil {
		return nil, 0, false, err
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
		startFrom := strings.TrimSpace(cfg.StartFrom)
		if startFrom == "" {
			startFrom = "deadline"
		}
		nowTS, err := parseDurableInt64(execCtx.Get("market.now_ts"), "market.now_ts")
		if err != nil {
			return nil, 0, false, err
		}
		anchorTS, err := parseDurableInt64(execCtx.Get("market."+startFrom), "market."+startFrom)
		if err != nil {
			return nil, 0, false, err
		}
		readyAt := anchorTS + int64(duration)
		outputs := map[string]string{
			"status":            "success",
			"mode":              "defer",
			"start_from":        startFrom,
			"anchor_ts":         strconv.FormatInt(anchorTS, 10),
			"ready_at":          strconv.FormatInt(readyAt, 10),
			"waited":            strconv.Itoa(duration),
			"remaining_seconds": "0",
		}
		if nowTS >= readyAt {
			return outputs, 0, true, nil
		}
		return outputs, readyAt, false, nil
	}
	outputs := map[string]string{
		"status": "success",
		"waited": strconv.Itoa(duration),
		"mode":   mode,
	}
	if duration == 0 {
		return outputs, 0, true, nil
	}
	return outputs, time.Now().Add(time.Duration(duration) * time.Second).Unix(), false, nil
}

func parseDurableInt64(raw string, name string) (int64, error) {
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return value, nil
}

func decodeNodeConfig[T any](raw interface{}) (T, error) {
	var out T
	data, err := json.Marshal(raw)
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return out, err
	}
	return out, nil
}

func isDurableSuspendNode(nodeType string) bool {
	return nodeType == "wait" || nodeType == "human_judge" || nodeType == "defer_resolution"
}

func isTerminalStatus(status RunStatus) bool {
	return status == RunStatusCompleted || status == RunStatusFailed || status == RunStatusCancelled
}

func nodeByID(bp dag.Blueprint, nodeID string) (dag.NodeDef, bool) {
	for _, node := range bp.Nodes {
		if node.ID == nodeID {
			return node, true
		}
	}
	return dag.NodeDef{}, false
}

func boolMapFromSet(values map[string]struct{}) map[string]bool {
	out := make(map[string]bool, len(values))
	for key := range values {
		out[key] = true
	}
	return out
}

func setFromBoolMap(values map[string]bool) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for key, value := range values {
		if value {
			out[key] = struct{}{}
		}
	}
	return out
}

func hasDueWaiting(waiting map[string]durableWaitingNode, now int64) bool {
	for _, node := range waiting {
		if node.ResumeAtUnix > 0 && node.ResumeAtUnix <= now {
			return true
		}
	}
	return false
}

func cloneWaitingMap(values map[string]durableWaitingNode) map[string]durableWaitingNode {
	out := make(map[string]durableWaitingNode, len(values))
	for key, value := range values {
		value.Outputs = cloneStringMap(value.Outputs)
		value.TimeoutOutputs = cloneStringMap(value.TimeoutOutputs)
		value.InputSnapshot = cloneStringMap(value.InputSnapshot)
		out[key] = value
	}
	return out
}

func sortedSignals(signals map[string]signalRequest) []signalRequest {
	keys := make([]string, 0, len(signals))
	for key := range signals {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]signalRequest, 0, len(keys))
	for _, key := range keys {
		out = append(out, signals[key])
	}
	return out
}

func waitingMatchesSignal(waiting durableWaitingNode, signal signalRequest) bool {
	if waiting.Kind != "signal" {
		return false
	}
	if waiting.CorrelationKey != "" && signal.CorrelationKey != waiting.CorrelationKey {
		return false
	}
	if waiting.SignalType == "" {
		return true
	}
	return signal.SignalType == waiting.SignalType || strings.HasPrefix(signal.SignalType, waiting.SignalType+".")
}

func validateSignalForWaiting(waiting durableWaitingNode, signal signalRequest) error {
	if waiting.SignalType == "human_judgment" && signal.SignalType == "human_judgment.responded" {
		if strings.TrimSpace(signal.Payload["outcome"]) == "" {
			return errors.New("human_judgment.responded signal requires payload.outcome")
		}
	}
	return nil
}

func outputsForSignal(waiting durableWaitingNode, signal signalRequest) map[string]string {
	outputs := cloneStringMap(signal.Payload)
	switch waiting.SignalType {
	case "human_judgment":
		if outputs["status"] == "" {
			outputs["status"] = "responded"
		}
	case "defer_resolution.resume":
		if outputs["status"] == "" {
			outputs["status"] = "success"
		}
		outputs["resumed"] = "true"
	default:
		if outputs["status"] == "" {
			outputs["status"] = "success"
		}
	}
	return outputs
}
