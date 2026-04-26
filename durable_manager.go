package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/qmrkt/resolution-engine/dag"
	"github.com/qmrkt/resolution-engine/executors"
)

type DurableRunManagerConfig struct {
	MaxWorkers   int
	MaxQueueSize int
	PollInterval time.Duration
	// TraceSink receives a snapshot envelope on every state-advancing save.
	// Optional — when nil, no observability traces are emitted.
	TraceSink traceSink
}

type DurableRunManager struct {
	runner        *Runner
	store         *durableFileStore
	client        *http.Client
	callbackToken string
	maxWorkers    int
	maxQueueSize  int
	pollInterval  time.Duration
	traceSink     traceSink

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	queue  chan string

	mu           sync.Mutex
	shuttingDown bool
	processing   map[string]bool
	// pending: enqueue attempts that arrived while markProcessing was held;
	// the holder re-enqueues on unmark so the dispatch is not lost.
	pending map[string]bool
	// queued: runs currently in the dispatch channel; the flag dedupes
	// enqueue bursts so the channel and blocked senders stay bounded.
	queued     map[string]bool
	runCancels map[string]context.CancelFunc
	// index is the in-memory projection of the run store's hot-path
	// metadata. Populated on startup by recoverRuns and kept in sync by
	// every successful persist. Eliminates O(H) directory scans on
	// Submit / Signal / timer paths.
	index *durableRunIndex
}

type queueFullError struct {
	MaxQueueSize int
}

var errDurableManagerShuttingDown = errors.New("run manager is shutting down")

// durableInvocationFromRecord builds a typed Invocation from a durable
// checkpoint. Inputs come from the persisted run inputs; Results are loaded
// directly from the typed Checkpoint.Run.Results.
func durableInvocationFromRecord(record *durableRunRecord) *dag.Invocation {
	if record == nil || record.Checkpoint.Run == nil {
		return dag.NewInvocation(dag.Run{})
	}
	startedAt, _ := time.Parse(time.RFC3339Nano, record.Checkpoint.Run.StartedAt)
	if startedAt.IsZero() {
		startedAt, _ = time.Parse(time.RFC3339, record.Checkpoint.Run.StartedAt)
	}
	inv := dag.NewInvocation(dag.Run{
		ID:          record.Checkpoint.Run.ID,
		BlueprintID: record.Checkpoint.Run.BlueprintID,
		StartedAt:   startedAt,
		Inputs:      cloneStringMap(record.Checkpoint.Run.Inputs),
	})
	if record.Checkpoint.Run.Results != nil {
		inv.Results.LoadSnapshot(record.Checkpoint.Run.Results.Snapshot())
	}
	return inv
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
		traceSink:     cfg.TraceSink,
		ctx:           ctx,
		cancel:        cancel,
		queue:         make(chan string, cfg.MaxQueueSize),
		processing:    make(map[string]bool),
		pending:       make(map[string]bool),
		queued:        make(map[string]bool),
		runCancels:    make(map[string]context.CancelFunc),
		index:         newDurableRunIndex(),
	}
	if err := manager.recoverRuns(); err != nil {
		cancel()
		return nil, err
	}
	if agent := runner.AgentExecutor(); agent != nil {
		agent.SetSignalFn(manager.deliverAgentSignal)
		agent.SetAsyncBaseContext(manager.ctx)
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

// deliverAgentSignal forwards async-agent completion through Signal.
func (m *DurableRunManager) deliverAgentSignal(runID, correlationKey string, payload map[string]string, usage dag.TokenUsage) error {
	if m == nil {
		return errors.New("durable run manager is nil")
	}
	const maxAttempts = 8
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		_, err := m.Signal(signalRequest{
			IdempotencyKey: correlationKey,
			RunID:          runID,
			SignalType:     executors.AgentDoneSignalType,
			CorrelationKey: correlationKey,
			Payload:        payload,
			Usage:          usage,
		})
		if err == nil {
			return nil
		}
		if !errors.Is(err, errDurableStaleRecord) {
			return err
		}
		lastErr = err
		time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
	}
	return lastErr
}

func (m *DurableRunManager) Close() {
	if m == nil {
		return
	}
	m.BeginShutdown()
	m.mu.Lock()
	for _, cancel := range m.runCancels {
		cancel()
	}
	m.mu.Unlock()
	m.wg.Wait()
}

// BeginShutdown stops new mutating work and cancels the manager context.
func (m *DurableRunManager) BeginShutdown() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.shuttingDown = true
	m.mu.Unlock()
	m.cancelAllAsyncExecutors()
	m.cancel()
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
		if _, ok := m.runner.engine.ExecutorFor(node.Type); !ok {
			return RunResult{}, fmt.Errorf("no executor registered for node type %q", node.Type)
		}
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	inputs := make(map[string]string, len(req.Inputs))
	for key, value := range req.Inputs {
		if strings.TrimSpace(key) == "" {
			continue
		}
		inputs[key] = value
	}
	scheduler := dag.NewScheduler(bp)
	run := &dag.RunState{
		ID:             req.RunID,
		BlueprintID:    bp.ID,
		Definition:     bp,
		Status:         string(RunStatusQueued),
		Inputs:         cloneStringMap(inputs),
		NodeStates:     make(map[string]dag.NodeState, len(bp.Nodes)),
		Results:        dag.NewResults(),
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
			RunState:      run,
		},
		Checkpoint: durableCheckpoint{
			Run:            run,
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
	if m.shuttingDown {
		m.mu.Unlock()
		return RunResult{}, errDurableManagerShuttingDown
	}
	if existing, ok, err := m.activeRunForApp(req.AppID); err != nil {
		m.mu.Unlock()
		return RunResult{}, err
	} else if ok {
		m.mu.Unlock()
		return RunResult{}, &duplicateRunError{RunID: existing.Request.RunID}
	}
	if m.maxQueueSize > 0 && m.index.QueuedCount() >= m.maxQueueSize {
		m.mu.Unlock()
		return RunResult{}, &queueFullError{MaxQueueSize: m.maxQueueSize}
	}
	if err := m.persist(record); err != nil {
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

	m.cancelAsyncRun(runID)
	applyCancel := func(r *durableRunRecord) error {
		if r == nil {
			return errors.New("run record is required")
		}
		ensureDurableRecordMaps(r)
		now := time.Now().UTC().Format(time.RFC3339Nano)
		r.Result.Status = RunStatusCancelled
		r.Result.Return = nil
		r.Result.Error = "run cancelled"
		r.CompletedAt = now
		// Cancel in-flight work before clearing Waiting.
		for _, waiting := range r.Checkpoint.Waiting {
			m.cancelInFlightExecutor(waiting)
		}
		r.Checkpoint.Waiting = make(map[string]durableWaitingNode)
		if r.Checkpoint.Run != nil {
			r.Checkpoint.Run.Status = string(RunStatusCancelled)
			r.Checkpoint.Run.Error = "run cancelled"
			r.Checkpoint.Run.CompletedAt = now
			r.Result.RunState = r.Checkpoint.Run
		}
		return nil
	}
	if err := applyCancel(record); err != nil {
		m.mu.Unlock()
		return RunResult{}, false
	}
	if err := m.persistWithRetry(record, applyCancel); err != nil {
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
	if m == nil {
		return 0
	}
	return m.index.ActiveCount()
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
	if m.shuttingDown {
		m.mu.Unlock()
		return signalResult{}, errDurableManagerShuttingDown
	}
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
			if err := m.completeWaitingNode(record, nodeID, outputsForSignal(waiting, req), req.Usage); err != nil {
				m.mu.Unlock()
				return signalResult{}, err
			}
			matched = true
		}
	}
	if matched {
		consumeSignal(record.Signals, req.IdempotencyKey, time.Now().UTC().Format(time.RFC3339Nano))
	}
	if matched && !isTerminalStatus(record.Result.Status) {
		record.Result.Status = RunStatusQueued
		if record.Checkpoint.Run != nil {
			record.Checkpoint.Run.Status = string(RunStatusQueued)
			record.Result.RunState = record.Checkpoint.Run
		}
	}
	if err := m.persist(record); err != nil {
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
			m.clearQueued(runID)
			m.processRunOnce(runID)
		}
	}
}

func (m *DurableRunManager) clearQueued(runID string) {
	m.mu.Lock()
	delete(m.queued, runID)
	m.mu.Unlock()
}

func (m *DurableRunManager) processRunOnce(runID string) {
	if !m.markProcessing(runID) {
		// markProcessing set the pending flag; the holder will re-enqueue.
		return
	}
	defer func() {
		if m.unmarkProcessing(runID) {
			m.enqueue(runID)
		}
	}()

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

	if err := m.advanceRun(runCtx, runID); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, errDurableStaleRecord) {
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
		_ = m.persist(record)
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
	if err := m.persist(record); err != nil {
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
			return m.persist(record)
		}

		ready := m.readyNodes(record)
		if len(ready) == 0 {
			if len(record.Checkpoint.Waiting) > 0 {
				record.Result.Status = RunStatusWaiting
				if record.Checkpoint.Run != nil {
					record.Checkpoint.Run.Status = string(RunStatusWaiting)
					record.Result.RunState = record.Checkpoint.Run
				}
				if err := m.persist(record); err != nil {
					return err
				}
				_ = m.store.appendEvent(runID, "RunWaiting", map[string]interface{}{"waiting": record.Checkpoint.Waiting})
				return nil
			}
			return m.completeRun(record)
		}

		execs, err := m.executeReadyBatch(ctx, record, ready)
		applyFn := func(r *durableRunRecord) error {
			return applyBatchExecutions(m, r, execs)
		}
		if err != nil {
			// Persist any sibling outcomes that finished before interruption.
			if len(execs) > 0 {
				if err := applyFn(record); err != nil {
					return err
				}
				for i := range execs {
					m.logNodeExecution(record, execs[i])
				}
				if err := m.persistWithRetry(record, applyFn); err != nil {
					return err
				}
			}
			return err
		}
		if err := applyFn(record); err != nil {
			return err
		}
		for i := range execs {
			m.logNodeExecution(record, execs[i])
		}
		if err := m.persistWithRetry(record, applyFn); err != nil {
			return err
		}
		if isTerminalStatus(record.Result.Status) {
			for i := range execs {
				if execs[i].EarlyReturn != nil {
					_ = m.store.appendEvent(runID, "RunCompletedViaReturn", map[string]interface{}{
						"node_id": execs[i].NodeID,
						"return":  cloneJSONRawMessage(*execs[i].EarlyReturn),
					})
					break
				}
			}
			return nil
		}
		if len(record.Checkpoint.Waiting) > 0 {
			record.Result.Status = RunStatusWaiting
			if record.Checkpoint.Run != nil {
				record.Checkpoint.Run.Status = string(RunStatusWaiting)
				record.Result.RunState = record.Checkpoint.Run
			}
			if err := m.persist(record); err != nil {
				return err
			}
			_ = m.store.appendEvent(runID, "RunWaiting", map[string]interface{}{"waiting": record.Checkpoint.Waiting})
			return nil
		}
	}
}

// applyBatchExecutions applies non-return outcomes before early returns.
func applyBatchExecutions(m *DurableRunManager, record *durableRunRecord, execs []nodeExecution) error {
	earlyReturns := make([]int, 0, len(execs))
	for i := range execs {
		if execs[i].EarlyReturn != nil {
			earlyReturns = append(earlyReturns, i)
			continue
		}
		if err := m.applyNodeExecution(record, execs[i]); err != nil {
			return err
		}
	}
	for _, i := range earlyReturns {
		if err := m.applyNodeExecution(record, execs[i]); err != nil {
			return err
		}
	}
	return nil
}

// nodeExecution is the outcome of a node attempt.
type nodeExecution struct {
	NodeID        string
	Node          dag.NodeDef
	StartedAt     string
	CompletedAt   string
	InputSnapshot map[string]string
	Iteration     int
	Kind          nodeExecutionKind
	Outputs       map[string]string
	Usage         dag.TokenUsage
	EarlyReturn   *json.RawMessage
	Err           error
	Waiting       durableWaitingNode
	MatchedSignal string
}

type nodeExecutionKind string

const (
	nodeExecCompleted nodeExecutionKind = "completed"
	nodeExecFailed    nodeExecutionKind = "failed"
	nodeExecSuspended nodeExecutionKind = "suspended"
)

// executeReadyBatch runs every ready node concurrently and returns outcomes in input order.
func (m *DurableRunManager) executeReadyBatch(ctx context.Context, record *durableRunRecord, ready []string) ([]nodeExecution, error) {
	if len(ready) == 0 {
		return nil, nil
	}

	execs := make([]nodeExecution, len(ready))
	executors := make([]dag.Executor, len(ready))
	invs := make([]*dag.Invocation, len(ready))
	for i, nodeID := range ready {
		inv := durableInvocationFromRecord(record)
		exec, executor, err := m.prepareNodeExecution(record, nodeID, inv)
		if err != nil {
			return nil, err
		}
		execs[i] = exec
		executors[i] = executor
		invs[i] = inv
	}
	record.Result.RunState = record.Checkpoint.Run
	if err := m.persist(record); err != nil {
		return nil, err
	}
	for i, nodeID := range ready {
		_ = m.store.appendEvent(record.Request.RunID, "NodeStarted", map[string]interface{}{
			"node_id":   nodeID,
			"iteration": execs[i].Iteration,
		})
	}

	type batchResult struct {
		index int
		exec  nodeExecution
		err   error
	}

	batchCtx, cancelBatch := context.WithCancel(ctx)
	defer cancelBatch()
	results := make(chan batchResult, len(ready))
	var wg sync.WaitGroup
	for i := range ready {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			result := batchResult{index: i}
			defer func() {
				if r := recover(); r != nil {
					result.exec = execs[i]
					result.exec.Kind = nodeExecFailed
					result.exec.Err = fmt.Errorf("executor panicked: %v", r)
					result.exec.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
					results <- result
				}
			}()
			result.exec, result.err = m.runNodeExecutor(batchCtx, execs[i], executors[i], invs[i], record)
			results <- result
		}(i)
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	earlyReturnWinner := -1
	var interruptedErr error
	for result := range results {
		// First early return wins.
		if earlyReturnWinner >= 0 && result.index != earlyReturnWinner {
			continue
		}

		execs[result.index] = result.exec
		if result.exec.EarlyReturn != nil && earlyReturnWinner < 0 {
			earlyReturnWinner = result.index
			cancelBatch()
			continue
		}

		if result.err != nil && errors.Is(result.err, context.Canceled) {
			// Ignore sibling cancellation after an early return.
			if ctx.Err() != nil {
				interruptedErr = result.err
			}
		}
	}
	if interruptedErr != nil {
		return execs, interruptedErr
	}
	return execs, nil
}

// prepareNodeExecution marks a node running and returns its seed execution.
func (m *DurableRunManager) prepareNodeExecution(record *durableRunRecord, nodeID string, inv *dag.Invocation) (nodeExecution, dag.Executor, error) {
	node, ok := nodeByID(record.Checkpoint.Run.Definition, nodeID)
	if !ok {
		return nodeExecution{}, nil, fmt.Errorf("node %q not found", nodeID)
	}
	executor, ok := m.runner.engine.ExecutorFor(node.Type)
	if !ok {
		return nodeExecution{}, nil, fmt.Errorf("no executor registered for node type %q", node.Type)
	}
	record.Checkpoint.IterationCount[nodeID]++
	iteration := record.Checkpoint.IterationCount[nodeID]
	startedAt := time.Now().UTC().Format(time.RFC3339Nano)
	inputSnapshot := dag.FlattenInvocation(inv)
	state := record.Checkpoint.Run.NodeStates[nodeID]
	state.Status = "running"
	state.StartedAt = startedAt
	state.CompletedAt = ""
	state.Error = ""
	state.Usage = dag.TokenUsage{}
	record.Checkpoint.Run.NodeStates[nodeID] = state
	return nodeExecution{
		NodeID:        nodeID,
		Node:          node,
		StartedAt:     startedAt,
		InputSnapshot: inputSnapshot,
		Iteration:     iteration,
	}, executor, nil
}

// runNodeExecutor executes a node without mutating the record.
func (m *DurableRunManager) runNodeExecutor(ctx context.Context, exec nodeExecution, executor dag.Executor, inv *dag.Invocation, record *durableRunRecord) (nodeExecution, error) {
	result, err := executor.Execute(ctx, exec.Node, inv)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return exec, err
		}
		exec.Kind = nodeExecFailed
		exec.Err = err
		exec.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return exec, nil
	}

	if result.EarlyReturn != nil {
		exec.Kind = nodeExecCompleted
		exec.EarlyReturn = cloneJSONRawMessagePtr(result.EarlyReturn)
		exec.Usage = result.Usage
		exec.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return exec, nil
	}

	if result.Suspend != nil {
		waiting := waitingFromSuspension(result.Suspend, result.Usage, exec.NodeID, exec.StartedAt, exec.InputSnapshot, exec.Iteration)
		if signalKey, outputs, usage, ok := findMatchingSignal(record.Signals, waiting); ok {
			exec.Kind = nodeExecCompleted
			exec.MatchedSignal = signalKey
			exec.Outputs = outputs
			exec.Usage = mergeTokenUsage(result.Usage, usage)
			exec.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
			return exec, nil
		}
		exec.Kind = nodeExecSuspended
		exec.Waiting = waiting
		exec.Usage = result.Usage
		return exec, nil
	}

	exec.Kind = nodeExecCompleted
	exec.Outputs = result.Outputs
	exec.Usage = result.Usage
	exec.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return exec, nil
}

// waitingFromSuspension adds engine bookkeeping to a suspension.
func waitingFromSuspension(s *dag.Suspension, usage dag.TokenUsage, nodeID, startedAt string, inputSnapshot map[string]string, iteration int) durableWaitingNode {
	return durableWaitingNode{
		NodeID:        nodeID,
		Suspension:    *s.Clone(),
		Usage:         usage,
		StartedAt:     startedAt,
		InputSnapshot: inputSnapshot,
		Iteration:     iteration,
	}
}

// findMatchingSignal returns the first pending signal that matches waiting.
func findMatchingSignal(signals map[string]signalRequest, waiting durableWaitingNode) (string, map[string]string, dag.TokenUsage, bool) {
	for _, signal := range sortedSignals(signals) {
		if signalConsumed(signal) {
			continue
		}
		if !waitingMatchesSignal(waiting, signal) {
			continue
		}
		if err := validateSignalForWaiting(waiting, signal); err != nil {
			continue
		}
		return signal.IdempotencyKey, outputsForSignal(waiting, signal), signal.Usage, true
	}
	return "", nil, dag.TokenUsage{}, false
}

// applyNodeExecution is safe to re-apply after a stale reload.
func (m *DurableRunManager) applyNodeExecution(record *durableRunRecord, exec nodeExecution) error {
	switch exec.Kind {
	case nodeExecCompleted:
		if exec.EarlyReturn != nil {
			return m.applyEarlyReturn(record, exec)
		}
		return m.applyCompletion(record, exec)
	case nodeExecFailed:
		m.applyFailure(record, exec)
		return nil
	case nodeExecSuspended:
		return m.applySuspension(record, exec)
	}
	return nil
}

func (m *DurableRunManager) applyCompletion(record *durableRunRecord, exec nodeExecution) error {
	outputs := exec.Outputs
	if outputs == nil {
		outputs = map[string]string{}
	}
	if exec.MatchedSignal != "" {
		consumeSignal(record.Signals, exec.MatchedSignal, exec.CompletedAt)
	}
	if record.Checkpoint.Run.Results == nil {
		record.Checkpoint.Run.Results = dag.NewResults()
	}
	record.Checkpoint.Run.Results.MergeOutputs(exec.Node.ID, outputs)
	if status, _ := record.Checkpoint.Run.Results.Get(exec.Node.ID, "status"); status == "" {
		record.Checkpoint.Run.Results.SetField(exec.Node.ID, "status", "completed")
	}

	completedAt := exec.CompletedAt
	if completedAt == "" {
		completedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	state := record.Checkpoint.Run.NodeStates[exec.Node.ID]
	state.Status = "completed"
	state.CompletedAt = completedAt
	state.Error = ""
	state.Usage = exec.Usage
	record.Checkpoint.Run.NodeStates[exec.Node.ID] = state
	record.Checkpoint.Run.NodeTraces = append(record.Checkpoint.Run.NodeTraces, dag.NodeTrace{
		NodeID:        exec.Node.ID,
		NodeType:      exec.Node.Type,
		NodeLabel:     exec.Node.Label,
		Iteration:     exec.Iteration,
		Status:        "completed",
		InputSnapshot: cloneStringMap(exec.InputSnapshot),
		Outputs:       cloneStringMap(outputs),
		Usage:         exec.Usage,
		StartedAt:     exec.StartedAt,
		CompletedAt:   completedAt,
	})
	record.Checkpoint.Run.Usage.InputTokens += exec.Usage.InputTokens
	record.Checkpoint.Run.Usage.OutputTokens += exec.Usage.OutputTokens
	record.Checkpoint.Completed[exec.Node.ID] = true
	delete(record.Checkpoint.Failed, exec.Node.ID)
	delete(record.Checkpoint.Waiting, exec.Node.ID)
	if err := m.evaluateOutgoingEdges(record, exec.Node.ID); err != nil {
		return err
	}
	record.Result.RunState = record.Checkpoint.Run
	return nil
}

func (m *DurableRunManager) applyEarlyReturn(record *durableRunRecord, exec nodeExecution) error {
	if err := m.applyCompletion(record, exec); err != nil {
		return err
	}
	completedAt := exec.CompletedAt
	if completedAt == "" {
		completedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	for _, waiting := range record.Checkpoint.Waiting {
		m.cancelInFlightExecutor(waiting)
	}
	record.Checkpoint.Waiting = make(map[string]durableWaitingNode)
	m.cancelAsyncRun(record.Request.RunID)
	markDurableSkippedNodes(record, completedAt)
	record.Checkpoint.Run.Return = cloneJSONRawMessage(*exec.EarlyReturn)
	record.Checkpoint.Run.Status = "completed"
	record.Checkpoint.Run.Error = ""
	record.Checkpoint.Run.CompletedAt = completedAt
	record.Result = buildRunResult(record.Request, record.Checkpoint.Run, nil)
	record.Result.Status = RunStatusCompleted
	record.Result.RunState = record.Checkpoint.Run
	record.CompletedAt = completedAt
	return nil
}

func (m *DurableRunManager) applyFailure(record *durableRunRecord, exec nodeExecution) {
	if record.Checkpoint.Run.Results == nil {
		record.Checkpoint.Run.Results = dag.NewResults()
	}
	record.Checkpoint.Run.Results.SetField(exec.Node.ID, "status", "failed")
	record.Checkpoint.Run.Results.SetField(exec.Node.ID, "error", exec.Err.Error())
	completedAt := exec.CompletedAt
	if completedAt == "" {
		completedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	state := record.Checkpoint.Run.NodeStates[exec.Node.ID]
	state.Status = "failed"
	state.CompletedAt = completedAt
	state.Error = exec.Err.Error()
	record.Checkpoint.Run.NodeStates[exec.Node.ID] = state
	record.Checkpoint.Run.NodeTraces = append(record.Checkpoint.Run.NodeTraces, dag.NodeTrace{
		NodeID:        exec.Node.ID,
		NodeType:      exec.Node.Type,
		NodeLabel:     exec.Node.Label,
		Iteration:     exec.Iteration,
		Status:        "failed",
		InputSnapshot: cloneStringMap(exec.InputSnapshot),
		Error:         exec.Err.Error(),
		StartedAt:     exec.StartedAt,
		CompletedAt:   completedAt,
	})
	if exec.Node.OnError == "continue" {
		record.Checkpoint.Completed[exec.Node.ID] = true
	} else {
		record.Checkpoint.Failed[exec.Node.ID] = true
	}
	record.Result.RunState = record.Checkpoint.Run
}

func (m *DurableRunManager) applySuspension(record *durableRunRecord, exec nodeExecution) error {
	if signalKey, outputs, usage, ok := findMatchingSignal(record.Signals, exec.Waiting); ok {
		exec.Kind = nodeExecCompleted
		exec.MatchedSignal = signalKey
		exec.Outputs = outputs
		exec.Usage = mergeTokenUsage(exec.Usage, usage)
		if exec.CompletedAt == "" {
			exec.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
		}
		return m.applyCompletion(record, exec)
	}
	state := record.Checkpoint.Run.NodeStates[exec.Node.ID]
	state.Status = "waiting"
	state.Usage = exec.Usage
	record.Checkpoint.Run.NodeStates[exec.Node.ID] = state
	record.Checkpoint.Waiting[exec.Node.ID] = exec.Waiting
	record.Result.RunState = record.Checkpoint.Run
	return nil
}

func (m *DurableRunManager) logNodeExecution(record *durableRunRecord, exec nodeExecution) {
	switch exec.Kind {
	case nodeExecCompleted:
		_ = m.store.appendEvent(record.Request.RunID, "NodeCompleted", map[string]interface{}{"node_id": exec.Node.ID, "outputs": exec.Outputs})
	case nodeExecFailed:
		_ = m.store.appendEvent(record.Request.RunID, "NodeFailed", map[string]interface{}{"node_id": exec.Node.ID, "error": exec.Err.Error()})
	case nodeExecSuspended:
		_ = m.store.appendEvent(record.Request.RunID, "NodeSuspended", exec.Waiting)
	}
}

// persistWithRetry reloads and reapplies on errDurableStaleRecord.
func (m *DurableRunManager) persistWithRetry(record *durableRunRecord, reapply func(*durableRunRecord) error) error {
	const maxAttempts = 5
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err := m.persist(record)
		if err == nil {
			return nil
		}
		if !errors.Is(err, errDurableStaleRecord) {
			return err
		}
		lastErr = err
		fresh, loadErr := m.store.loadRun(record.Request.RunID)
		if loadErr != nil {
			return loadErr
		}
		ensureDurableRecordMaps(fresh)
		if isTerminalStatus(fresh.Result.Status) {
			*record = *fresh
			return nil
		}
		if err := reapply(fresh); err != nil {
			return err
		}
		*record = *fresh
	}
	return lastErr
}

// completeWaitingNode resumes a waiting node in place.
func (m *DurableRunManager) completeWaitingNode(record *durableRunRecord, nodeID string, outputs map[string]string, resumeUsage dag.TokenUsage) error {
	waiting, ok := record.Checkpoint.Waiting[nodeID]
	if !ok {
		return nil
	}
	node, ok := nodeByID(record.Checkpoint.Run.Definition, nodeID)
	if !ok {
		return fmt.Errorf("node %q not found", nodeID)
	}
	exec := nodeExecution{
		NodeID:        node.ID,
		Node:          node,
		StartedAt:     waiting.StartedAt,
		InputSnapshot: waiting.InputSnapshot,
		Iteration:     waiting.Iteration,
		Kind:          nodeExecCompleted,
		Outputs:       outputs,
		Usage:         mergeTokenUsage(waiting.Usage, resumeUsage),
		CompletedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := m.applyCompletion(record, exec); err != nil {
		return err
	}
	m.logNodeExecution(record, exec)
	return nil
}

func (m *DurableRunManager) evaluateOutgoingEdges(record *durableRunRecord, nodeID string) error {
	inv := durableInvocationFromRecord(record)
	scheduler := record.ensureScheduler()
	edges, err := scheduler.EvaluateEdges(nodeID, inv)
	if err != nil {
		return err
	}
	for _, edge := range edges {
		scheduler.TrackTraversal(edge.From, edge.To)
		record.Checkpoint.Activated[edge.To] = true
		if record.Checkpoint.Completed[edge.To] {
			if record.Checkpoint.Run.Results != nil {
				record.Checkpoint.Run.Results.AppendHistory(edge.To)
				record.Checkpoint.Run.Results.Reset(edge.To)
			}
			delete(record.Checkpoint.Completed, edge.To)
			state := record.Checkpoint.Run.NodeStates[edge.To]
			state.Status = "pending"
			state.CompletedAt = ""
			state.Error = ""
			state.Usage = dag.TokenUsage{}
			record.Checkpoint.Run.NodeStates[edge.To] = state
		}
		// Back-edge re-entry cancels the old wait and resets the node.
		if waiting, ok := record.Checkpoint.Waiting[edge.To]; ok {
			if record.Checkpoint.Run.Results != nil {
				record.Checkpoint.Run.Results.AppendHistory(edge.To)
				record.Checkpoint.Run.Results.Reset(edge.To)
			}
			m.cancelInFlightExecutor(waiting)
			delete(record.Checkpoint.Waiting, edge.To)
			state := record.Checkpoint.Run.NodeStates[edge.To]
			state.Status = "pending"
			state.CompletedAt = ""
			state.Error = ""
			state.Usage = dag.TokenUsage{}
			record.Checkpoint.Run.NodeStates[edge.To] = state
			_ = m.store.appendEvent(record.Request.RunID, "NodeResetFromWaiting", map[string]interface{}{
				"node_id":         edge.To,
				"correlation_key": waiting.CorrelationKey,
			})
		}
	}
	record.Checkpoint.EdgeTraversals = scheduler.TraversalSnapshot()
	record.Checkpoint.Run.EdgeTraversals = cloneIntMap(record.Checkpoint.EdgeTraversals)
	return nil
}

// cancelInFlightExecutor invokes the correlation-keyed cancel on any
// registered CancellableExecutor. No-op for waiting entries whose signal
// correlation has no in-flight goroutine behind it (wait, await_signal).
func (m *DurableRunManager) cancelInFlightExecutor(waiting durableWaitingNode) {
	if m == nil || m.runner == nil || m.runner.engine == nil {
		return
	}
	m.runner.engine.CancelCorrelation(waiting.CorrelationKey)
}

func (m *DurableRunManager) cancelAsyncRun(runID string) {
	if m == nil || m.runner == nil {
		return
	}
	if agent := m.runner.AgentExecutor(); agent != nil {
		agent.CancelRun(runID)
	}
}

func (m *DurableRunManager) cancelAllAsyncExecutors() {
	if m == nil || m.runner == nil {
		return
	}
	if agent := m.runner.AgentExecutor(); agent != nil {
		agent.CancelAll()
	}
}

func markDurableSkippedNodes(record *durableRunRecord, completedAt string) {
	if record == nil || record.Checkpoint.Run == nil {
		return
	}
	for nodeID, state := range record.Checkpoint.Run.NodeStates {
		switch state.Status {
		case "pending", "waiting", "running":
			state.Status = "skipped"
			state.CompletedAt = completedAt
			state.Error = ""
			record.Checkpoint.Run.NodeStates[nodeID] = state
			record.Checkpoint.Skipped[nodeID] = true
			delete(record.Checkpoint.Waiting, nodeID)
		}
	}
}

func (m *DurableRunManager) completeRun(record *durableRunRecord) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	record.Checkpoint.Run.EdgeTraversals = cloneIntMap(record.Checkpoint.EdgeTraversals)
	record.Checkpoint.Run.CompletedAt = now
	if len(record.Checkpoint.Failed) > 0 {
		record.Checkpoint.Run.Status = "failed"
		if record.Checkpoint.Run.Error == "" {
			record.Checkpoint.Run.Error = "one or more nodes failed"
		}
		record.Result = buildRunResult(record.Request, record.Checkpoint.Run, errors.New(record.Checkpoint.Run.Error))
	} else {
		if len(record.Checkpoint.Run.Return) == 0 && m.runner.engine.BlueprintRequiresTerminalFire(record.Checkpoint.Run.Definition) {
			record.Checkpoint.Run.Status = "failed"
			record.Checkpoint.Run.Error = "blueprint completed without firing return"
			record.Result = buildRunResult(record.Request, record.Checkpoint.Run, errors.New(record.Checkpoint.Run.Error))
		} else {
			record.Checkpoint.Run.Status = "completed"
			record.Checkpoint.Run.Error = ""
			record.Result = buildRunResult(record.Request, record.Checkpoint.Run, nil)
		}
	}
	record.Result.RunState = record.Checkpoint.Run
	record.CompletedAt = now
	_, _ = m.runner.WriteEvidence(record.Request.AppID, record.Checkpoint.Run)
	if err := m.persist(record); err != nil {
		return err
	}
	_ = m.store.appendEvent(record.Request.RunID, "RunCompleted", map[string]interface{}{"status": record.Result.Status})
	return nil
}

func (m *DurableRunManager) readyNodes(record *durableRunRecord) []string {
	scheduler := record.ensureScheduler()
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
	now := time.Now().Unix()
	for _, runID := range m.index.DueTimerRunIDs(now) {
		record, err := m.store.loadRun(runID)
		if err != nil {
			return err
		}
		ensureDurableRecordMaps(record)
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
		if err := m.persist(record); err != nil {
			return err
		}
		m.enqueue(record.Request.RunID)
	}
	return nil
}

func (m *DurableRunManager) applyDueWaiting(record *durableRunRecord, now int64) error {
	// Snapshot keys so completion can delete from Waiting safely.
	dueNodeIDs := make([]string, 0, len(record.Checkpoint.Waiting))
	for nodeID := range record.Checkpoint.Waiting {
		dueNodeIDs = append(dueNodeIDs, nodeID)
	}
	for _, nodeID := range dueNodeIDs {
		waiting, ok := record.Checkpoint.Waiting[nodeID]
		if !ok {
			continue
		}
		if waiting.ResumeAtUnix <= 0 || waiting.ResumeAtUnix > now {
			continue
		}
		outputs := waiting.Outputs
		if waiting.Kind == dag.SuspensionKindSignal && len(waiting.TimeoutOutputs) > 0 {
			outputs = waiting.TimeoutOutputs
		}
		if outputs == nil {
			continue
		}
		if err := m.completeWaitingNode(record, nodeID, outputs, dag.TokenUsage{}); err != nil {
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
	now := time.Now().UTC()
	for _, runID := range m.index.PendingCallbackRunIDs(now.Unix()) {
		record, err := m.store.loadRun(runID)
		if err != nil {
			return err
		}
		// Defensive race guard: a concurrent persist could have marked the
		// callback delivered (or cleared the URL) between the index read
		// and the loadRun. The index filter already excluded these.
		if !isTerminalStatus(record.Result.Status) || record.CallbackDelivered || strings.TrimSpace(record.Request.CallbackURL) == "" {
			continue
		}
		if err := m.postCallback(record.Request.CallbackURL, record.Result); err != nil {
			record.CallbackAttempts++
			delay := time.Duration(record.CallbackAttempts) * time.Second
			if delay > DefaultCallbackMaxBackoff {
				delay = DefaultCallbackMaxBackoff
			}
			record.NextCallbackAt = now.Add(delay).Format(time.RFC3339Nano)
			if err := m.store.saveRun(record); err == nil {
				m.index.Upsert(record)
			}
			_ = m.store.appendEvent(record.Request.RunID, "CallbackFailed", map[string]string{"error": err.Error()})
			continue
		}
		record.CallbackDelivered = true
		record.NextCallbackAt = ""
		if err := m.store.saveRun(record); err != nil {
			return err
		}
		m.index.Upsert(record)
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
		// Index every run (including terminal ones) so callback delivery
		// and signal lookup can query the index instead of re-scanning
		// the directory.
		m.index.Upsert(record)
		if isTerminalStatus(record.Result.Status) {
			continue
		}
		ensureDurableRecordMaps(record)

		// Fail waits whose in-process backing died with the previous process.
		interrupted, err := m.failInterruptedWaits(record)
		if err != nil {
			return err
		}

		// Re-queue interrupted runs and runs with failed waits.
		if record.Result.Status == RunStatusRunning || interrupted > 0 {
			record.Result.Status = RunStatusQueued
			if record.Checkpoint.Run != nil {
				record.Checkpoint.Run.Status = string(RunStatusQueued)
				record.Result.RunState = record.Checkpoint.Run
			}
			if err := m.persist(record); err != nil {
				return err
			}
			_ = m.store.appendEvent(record.Request.RunID, "RunRecovered", map[string]interface{}{
				"interrupted_waits": interrupted,
			})
		}

		if record.Result.Status == RunStatusQueued {
			m.enqueue(record.Request.RunID)
		}
	}
	return nil
}

// failInterruptedWaits fails waits backed by in-process state that died on restart.
func (m *DurableRunManager) failInterruptedWaits(record *durableRunRecord) (int, error) {
	if record == nil || len(record.Checkpoint.Waiting) == 0 {
		return 0, nil
	}
	type interruptedWait struct {
		nodeID         string
		owner          string
		correlationKey string
	}
	// Snapshot first: completeWaitingNode mutates Checkpoint.Waiting.
	toFail := make([]interruptedWait, 0, len(record.Checkpoint.Waiting))
	for nodeID, waiting := range record.Checkpoint.Waiting {
		owner := strings.TrimSpace(waiting.RecoveryOwner)
		if owner == "" {
			continue
		}
		toFail = append(toFail, interruptedWait{
			nodeID:         nodeID,
			owner:          owner,
			correlationKey: waiting.CorrelationKey,
		})
	}
	for _, w := range toFail {
		outputs := waitingInterruptedByRestartOutputs(w.owner)
		if err := m.completeWaitingNode(record, w.nodeID, outputs, dag.TokenUsage{}); err != nil {
			return 0, fmt.Errorf("recover run %s: fail interrupted wait on node %s: %w", record.Request.RunID, w.nodeID, err)
		}
		_ = m.store.appendEvent(record.Request.RunID, "WaitingNodeInterruptedByRestart", map[string]interface{}{
			"node_id":         w.nodeID,
			"owner":           w.owner,
			"correlation_key": w.correlationKey,
		})
	}
	return len(toFail), nil
}

// waitingInterruptedByRestartOutputs returns the failure payload for restart-lost waits.
func waitingInterruptedByRestartOutputs(owner string) map[string]string {
	return map[string]string{
		"status":  "failed",
		"error":   fmt.Sprintf("%s wait interrupted by manager restart", owner),
		"outcome": "inconclusive",
	}
}

func (m *DurableRunManager) activeRunForApp(appID int) (*durableRunRecord, bool, error) {
	runIDs := m.index.ActiveRunIDsByApp(appID)
	if len(runIDs) == 0 {
		return nil, false, nil
	}
	record, err := m.store.loadRun(runIDs[0])
	if err != nil {
		return nil, false, err
	}
	// Defensive: the index is updated after every persist, but a read
	// here might race a concurrent terminal transition. Re-check.
	if record == nil || isTerminalStatus(record.Result.Status) {
		return nil, false, nil
	}
	return record, true, nil
}

func (m *DurableRunManager) findSignalTarget(req signalRequest) (*durableRunRecord, error) {
	if strings.TrimSpace(req.RunID) != "" {
		return m.store.loadRun(req.RunID)
	}
	var candidates []string
	if req.AppID > 0 {
		candidates = m.index.ActiveRunIDsByApp(req.AppID)
	} else {
		candidates = m.index.allActiveRunIDs()
	}
	for _, runID := range candidates {
		run, err := m.store.loadRun(runID)
		if err != nil {
			return nil, err
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
	_ = m.persist(record)
	_ = m.store.appendEvent(runID, "RunInterrupted", map[string]string{"error": err.Error()})
	return err
}

// enqueue dispatches runID to a worker with per-run dedup.
func (m *DurableRunManager) enqueue(runID string) {
	m.mu.Lock()
	if m.queued[runID] {
		m.mu.Unlock()
		return
	}
	if m.processing[runID] {
		m.pending[runID] = true
		m.mu.Unlock()
		return
	}
	m.queued[runID] = true
	m.mu.Unlock()

	select {
	case m.queue <- runID:
	case <-m.ctx.Done():
		m.clearQueued(runID)
	default:
		// At most one blocked sender per runID.
		go func() {
			select {
			case m.queue <- runID:
			case <-m.ctx.Done():
				m.clearQueued(runID)
			}
		}()
	}
}

// persist saves the record, updates the in-memory index, and emits a trace
// snapshot. Bookkeeping-only saves that live outside this function (e.g.
// callback delivery retry metadata) must call m.index.Upsert themselves
// after a successful saveRun.
func (m *DurableRunManager) persist(record *durableRunRecord) error {
	if err := m.store.saveRun(record); err != nil {
		return err
	}
	m.index.Upsert(record)
	m.emitTrace(record)
	return nil
}

// emitTrace sends a defensive snapshot of the current run state to the trace
// sink. Best-effort: the sink may drop envelopes under pressure.
func (m *DurableRunManager) emitTrace(record *durableRunRecord) {
	if m.traceSink == nil || record == nil || record.Checkpoint.Run == nil {
		return
	}
	m.traceSink.Enqueue(TraceEnvelope{
		AppID:         record.Request.AppID,
		BlueprintPath: strings.TrimSpace(record.Request.BlueprintPath),
		Initiator:     strings.TrimSpace(record.Request.Initiator),
		Revision:      record.Revision,
		Run:           cloneDAGRunState(record.Checkpoint.Run),
	})
}

// markProcessing claims exclusive processing for runID. If another worker
// already holds it, the claim fails but the pending flag is raised so the
// holder re-enqueues the run on its way out.
func (m *DurableRunManager) markProcessing(runID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.processing[runID] {
		m.pending[runID] = true
		return false
	}
	m.processing[runID] = true
	delete(m.pending, runID)
	return true
}

// unmarkProcessing releases the claim and reports whether another dispatch
// arrived while the claim was held. The caller is expected to re-enqueue when
// this returns true.
func (m *DurableRunManager) unmarkProcessing(runID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.processing, runID)
	if m.pending[runID] {
		delete(m.pending, runID)
		return true
	}
	return false
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
	if waiting.Kind != dag.SuspensionKindSignal {
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
	for _, key := range waiting.RequiredPayload {
		if strings.TrimSpace(key) == "" {
			continue
		}
		if strings.TrimSpace(signal.Payload[key]) == "" {
			return fmt.Errorf("%s signal requires payload.%s", signal.SignalType, key)
		}
	}
	return nil
}

func signalConsumed(signal signalRequest) bool {
	return strings.TrimSpace(signal.ConsumedAt) != ""
}

func consumeSignal(signals map[string]signalRequest, key string, consumedAt string) {
	if signals == nil || strings.TrimSpace(key) == "" {
		return
	}
	signal, ok := signals[key]
	if !ok || signalConsumed(signal) {
		return
	}
	if strings.TrimSpace(consumedAt) == "" {
		consumedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	signal.ConsumedAt = consumedAt
	signals[key] = signal
}

func outputsForSignal(waiting durableWaitingNode, signal signalRequest) map[string]string {
	outputs := cloneStringMap(waiting.DefaultOutputs)
	if outputs == nil {
		outputs = make(map[string]string, len(signal.Payload)+1)
	}
	for key, value := range signal.Payload {
		outputs[key] = value
	}
	if outputs["status"] == "" {
		outputs["status"] = "success"
	}
	return outputs
}

func mergeTokenUsage(base, extra dag.TokenUsage) dag.TokenUsage {
	base.InputTokens += extra.InputTokens
	base.OutputTokens += extra.OutputTokens
	return base
}
