package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

// NodeExecutor executes a single node in the DAG.
type NodeExecutor interface {
	Execute(ctx context.Context, node NodeDef, execCtx *Context) (ExecutorResult, error)
}

// RunObserver receives defensive copies of a run as execution progresses.
type RunObserver interface {
	OnRunSnapshot(run *RunState)
}

// Engine runs resolution logic DAGs with registered executors.
type Engine struct {
	mu        sync.RWMutex
	executors map[string]NodeExecutor
	logger    *slog.Logger
}

// NewEngine creates a DAG execution engine.
func NewEngine(logger *slog.Logger) *Engine {
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		executors: make(map[string]NodeExecutor),
		logger:    logger,
	}
}

// RegisterExecutor registers an executor for a node type.
func (e *Engine) RegisterExecutor(nodeType string, exec NodeExecutor) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.executors[nodeType] = exec
}

func (e *Engine) getExecutor(nodeType string) (NodeExecutor, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	exec, ok := e.executors[nodeType]
	return exec, ok
}

// ExecutorFor returns the registered executor for a node type.
func (e *Engine) ExecutorFor(nodeType string) (NodeExecutor, bool) {
	return e.getExecutor(nodeType)
}

// Execute runs a blueprint to completion and returns the final run state.
func (e *Engine) Execute(ctx context.Context, bp Blueprint, inputs map[string]string) (*RunState, error) {
	return e.ExecuteWithObserver(ctx, bp, inputs, nil)
}

// ExecuteWithObserver runs a blueprint and emits defensive run snapshots to the observer.
func (e *Engine) ExecuteWithObserver(ctx context.Context, bp Blueprint, inputs map[string]string, observer RunObserver) (*RunState, error) {
	if errs := ValidateBlueprint(bp); len(errs) > 0 {
		return nil, fmt.Errorf("blueprint validation failed: %v", errs[0])
	}

	// Check all node types have registered executors
	for _, node := range bp.Nodes {
		if _, ok := e.getExecutor(node.Type); !ok {
			return nil, fmt.Errorf("no executor registered for node type %q", node.Type)
		}
	}

	// Apply budget timeout
	if bp.Budget != nil && bp.Budget.MaxTotalTimeSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(bp.Budget.MaxTotalTimeSeconds)*time.Second)
		defer cancel()
	}

	var cancelExecution context.CancelFunc
	if bp.Budget != nil && bp.Budget.MaxTotalTokens > 0 {
		ctx, cancelExecution = context.WithCancel(ctx)
		defer cancelExecution()
	}

	execCtx := NewContext(inputs)
	now := time.Now().UTC().Format(time.RFC3339)
	run := &RunState{
		ID:             uuid.New().String(),
		BlueprintID:    bp.ID,
		Definition:     bp,
		Status:         "running",
		NodeStates:     make(map[string]NodeState, len(bp.Nodes)),
		EdgeTraversals: make(map[string]int),
		StartedAt:      now,
	}

	execCtx.Set("resolution_run_id", run.ID)
	execCtx.Set("input.resolution_run_id", run.ID)
	execCtx.Set("__run_id", run.ID)
	run.Inputs = execCtx.Snapshot()
	run.Context = execCtx.Snapshot()

	for _, node := range bp.Nodes {
		run.NodeStates[node.ID] = NodeState{Status: "pending"}
	}
	notifyObserver(observer, run)

	err := e.runStateMachine(ctx, bp, run, execCtx, observer, cancelExecution)

	run.Context = execCtx.Snapshot()
	run.CompletedAt = time.Now().UTC().Format(time.RFC3339)

	if err != nil {
		run.Status = "failed"
		run.Error = err.Error()
		notifyObserver(observer, run)
		return run, err
	}

	// Determine final status — only count nodes that actually blocked execution
	anyHardFailed := false
	for _, node := range bp.Nodes {
		ns := run.NodeStates[node.ID]
		if ns.Status == "failed" && node.OnError != "continue" {
			anyHardFailed = true
			if run.Error == "" {
				run.Error = ns.Error
			}
		}
	}
	if anyHardFailed {
		run.Status = "failed"
	} else {
		run.Status = "completed"
		run.Error = ""
	}
	notifyObserver(observer, run)

	return run, nil
}

func (e *Engine) runStateMachine(
	ctx context.Context,
	bp Blueprint,
	run *RunState,
	execCtx *Context,
	observer RunObserver,
	cancelExecution context.CancelFunc,
) error {
	scheduler := NewScheduler(bp)
	nodeMap := make(map[string]NodeDef, len(bp.Nodes))
	for _, node := range bp.Nodes {
		nodeMap[node.ID] = node
	}
	iterationCount := make(map[string]int)
	inputSnapshots := make(map[string]map[string]string)
	startedAt := make(map[string]string)

	completed := make(map[string]struct{})
	failed := make(map[string]struct{})
	running := make(map[string]struct{})
	var budgetErr error
	// activated tracks nodes that have been reached via edge traversal.
	// Root nodes (no incoming forward edges) are activated by default.
	activated := make(map[string]struct{})
	for _, node := range bp.Nodes {
		hasForwardIncoming := false
		for _, edge := range scheduler.incoming[node.ID] {
			if !scheduler.IsBackEdge(edge.From, edge.To) {
				hasForwardIncoming = true
				break
			}
		}
		if !hasForwardIncoming {
			activated[node.ID] = struct{}{}
		}
	}

	for {
		if err := ctx.Err(); err != nil {
			run.EdgeTraversals = scheduler.TraversalSnapshot()
			run.Context = execCtx.Snapshot()
			if syncSkippedNodeStates(run, scheduler, time.Now().UTC().Format(time.RFC3339)) {
				notifyObserver(observer, run)
			}
			return fmt.Errorf("execution cancelled: %w", err)
		}

		ready := scheduler.ReadyNodes(completed, failed, running)
		run.Context = execCtx.Snapshot()
		if syncSkippedNodeStates(run, scheduler, time.Now().UTC().Format(time.RFC3339)) {
			notifyObserver(observer, run)
		}
		// Filter to only activated nodes
		filteredReady := make([]string, 0, len(ready))
		for _, nid := range ready {
			if _, ok := activated[nid]; ok {
				filteredReady = append(filteredReady, nid)
			}
		}
		ready = filteredReady

		if len(ready) == 0 && len(running) == 0 {
			break
		}

		if len(ready) == 0 {
			break
		}

		// Execute ready nodes in parallel
		type nodeResult struct {
			nodeID string
			result ExecutorResult
			err    error
		}

		resultCh := make(chan nodeResult, len(ready))
		var wg sync.WaitGroup

		for _, nodeID := range ready {
			running[nodeID] = struct{}{}
			iterationCount[nodeID]++
			inputSnapshots[nodeID] = execCtx.Snapshot()
			startedAt[nodeID] = time.Now().UTC().Format(time.RFC3339)

			state := run.NodeStates[nodeID]
			state.Status = "running"
			state.StartedAt = startedAt[nodeID]
			state.CompletedAt = ""
			state.Error = ""
			state.Usage = TokenUsage{}
			run.NodeStates[nodeID] = state

			wg.Add(1)
			go func(nid string) {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						resultCh <- nodeResult{
							nodeID: nid,
							err:    fmt.Errorf("executor panicked: %v", r),
						}
					}
				}()
				node := nodeMap[nid]
				exec, _ := e.getExecutor(node.Type)

				e.logger.Info("executing node", "node", nid, "type", node.Type)
				result, err := exec.Execute(ctx, node, execCtx)
				resultCh <- nodeResult{nodeID: nid, result: result, err: err}
			}(nodeID)
		}
		run.Context = execCtx.Snapshot()
		notifyObserver(observer, run)

		// Wait for all parallel nodes
		go func() {
			wg.Wait()
			close(resultCh)
		}()

		for nr := range resultCh {
			delete(running, nr.nodeID)
			completedAt := time.Now().UTC().Format(time.RFC3339)
			node := nodeMap[nr.nodeID]
			state := run.NodeStates[nr.nodeID]
			trace := NodeTrace{
				NodeID:        nr.nodeID,
				NodeType:      node.Type,
				NodeLabel:     node.Label,
				Iteration:     iterationCount[nr.nodeID],
				InputSnapshot: cloneStringMap(inputSnapshots[nr.nodeID]),
				StartedAt:     startedAt[nr.nodeID],
				CompletedAt:   completedAt,
			}

			if nr.err != nil {
				e.logger.Error("node failed", "node", nr.nodeID, "error", nr.err)

				state.Status = "failed"
				state.CompletedAt = completedAt
				state.Error = nr.err.Error()
				state.Usage = TokenUsage{}
				run.NodeStates[nr.nodeID] = state
				trace.Status = "failed"
				trace.Error = nr.err.Error()
				run.NodeTraces = append(run.NodeTraces, trace)

				execCtx.Set(nr.nodeID+".status", "failed")
				execCtx.Set(nr.nodeID+".error", nr.err.Error())

				if node.OnError != "continue" {
					failed[nr.nodeID] = struct{}{}
				} else {
					completed[nr.nodeID] = struct{}{}
				}
			} else {
				nextInputTokens := run.Usage.InputTokens + nr.result.Usage.InputTokens
				nextOutputTokens := run.Usage.OutputTokens + nr.result.Usage.OutputTokens
				nextTotalTokens := nextInputTokens + nextOutputTokens
				tokenBudgetExceeded := bp.Budget != nil &&
					bp.Budget.MaxTotalTokens > 0 &&
					nextTotalTokens > bp.Budget.MaxTotalTokens
				state.CompletedAt = completedAt
				state.Usage = nr.result.Usage

				// Merge outputs into context
				for key, value := range nr.result.Outputs {
					execCtx.Set(nr.nodeID+"."+key, value)
				}
				run.Usage.InputTokens = nextInputTokens
				run.Usage.OutputTokens = nextOutputTokens

				if tokenBudgetExceeded {
					budgetErr = fmt.Errorf(
						"token budget exceeded: used %d > max %d",
						nextTotalTokens,
						bp.Budget.MaxTotalTokens,
					)
					state.Status = "failed"
					state.Error = budgetErr.Error()
					run.NodeStates[nr.nodeID] = state
					trace.Status = "failed"
					trace.Outputs = cloneStringMap(nr.result.Outputs)
					trace.Error = budgetErr.Error()
					trace.Usage = nr.result.Usage
					run.NodeTraces = append(run.NodeTraces, trace)
					execCtx.Set(nr.nodeID+".status", "failed")
					execCtx.Set(nr.nodeID+".error", budgetErr.Error())
					failed[nr.nodeID] = struct{}{}
					if cancelExecution != nil {
						cancelExecution()
					}
				} else {
					state.Status = "completed"
					state.Error = ""
					run.NodeStates[nr.nodeID] = state
					trace.Status = "completed"
					trace.Outputs = cloneStringMap(nr.result.Outputs)
					trace.Usage = nr.result.Usage
					run.NodeTraces = append(run.NodeTraces, trace)
					// Only set engine status if the executor didn't set its own
					if execCtx.Get(nr.nodeID+".status") == "" {
						execCtx.Set(nr.nodeID+".status", "completed")
					}

					completed[nr.nodeID] = struct{}{}
				}
			}
			delete(inputSnapshots, nr.nodeID)
			delete(startedAt, nr.nodeID)
			if budgetErr != nil {
				run.Context = execCtx.Snapshot()
				notifyObserver(observer, run)
				continue
			}

			// Evaluate outgoing edges and activate targets
			edges, evalErr := scheduler.EvaluateEdges(nr.nodeID, execCtx)
			if evalErr != nil {
				e.logger.Error("edge condition error", "node", nr.nodeID, "error", evalErr)
				state := run.NodeStates[nr.nodeID]
				state.Status = "failed"
				state.Error = evalErr.Error()
				run.NodeStates[nr.nodeID] = state
				delete(completed, nr.nodeID)
				if node.OnError == "continue" {
					completed[nr.nodeID] = struct{}{}
				} else {
					failed[nr.nodeID] = struct{}{}
				}
			}
			for _, edge := range edges {
				scheduler.TrackTraversal(edge.From, edge.To)
				run.EdgeTraversals = scheduler.TraversalSnapshot()
				activated[edge.To] = struct{}{}

				// If target was already completed (back-edge), snapshot its
				// outputs into _runs history before resetting.
				if _, wasCompleted := completed[edge.To]; wasCompleted {
					execCtx.AppendNodeHistory(edge.To)
					delete(completed, edge.To)
					state := run.NodeStates[edge.To]
					state.Status = "pending"
					state.CompletedAt = ""
					state.Error = ""
					state.Usage = TokenUsage{}
					run.NodeStates[edge.To] = state
				}
			}
			run.Context = execCtx.Snapshot()
			notifyObserver(observer, run)
		}

		if budgetErr != nil {
			break
		}
	}
	run.EdgeTraversals = scheduler.TraversalSnapshot()
	run.Context = execCtx.Snapshot()
	if syncSkippedNodeStates(run, scheduler, time.Now().UTC().Format(time.RFC3339)) {
		notifyObserver(observer, run)
	}

	if budgetErr != nil {
		return budgetErr
	}

	// Detect orphaned pending nodes: activated but never completed/failed/skipped.
	// Only check when no hard failures occurred (those already cause a failed run).
	if len(failed) == 0 {
		var orphaned []string
		for _, node := range bp.Nodes {
			ns := run.NodeStates[node.ID]
			if ns.Status == "pending" {
				if _, wasActivated := activated[node.ID]; wasActivated {
					orphaned = append(orphaned, node.ID)
				}
			}
		}
		if len(orphaned) > 0 {
			return fmt.Errorf("orphaned pending nodes: %v", orphaned)
		}
	}

	return nil
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneIntMap(values map[string]int) map[string]int {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]int, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneRunState(run *RunState) *RunState {
	if run == nil {
		return nil
	}
	cloned := *run
	cloned.Inputs = cloneStringMap(run.Inputs)
	cloned.Context = cloneStringMap(run.Context)
	cloned.EdgeTraversals = cloneIntMap(run.EdgeTraversals)
	if len(run.NodeStates) > 0 {
		cloned.NodeStates = make(map[string]NodeState, len(run.NodeStates))
		for key, value := range run.NodeStates {
			cloned.NodeStates[key] = value
		}
	} else {
		cloned.NodeStates = nil
	}
	if len(run.NodeTraces) > 0 {
		cloned.NodeTraces = make([]NodeTrace, len(run.NodeTraces))
		for i, trace := range run.NodeTraces {
			trace.InputSnapshot = cloneStringMap(trace.InputSnapshot)
			trace.Outputs = cloneStringMap(trace.Outputs)
			cloned.NodeTraces[i] = trace
		}
	} else {
		cloned.NodeTraces = nil
	}
	cloned.Definition = cloneBlueprint(run.Definition)
	return &cloned
}

func cloneBlueprint(bp Blueprint) Blueprint {
	if len(bp.Nodes) == 0 && len(bp.Edges) == 0 && len(bp.Inputs) == 0 && bp.Budget == nil &&
		bp.ID == "" && bp.Name == "" && bp.Description == "" && bp.Version == 0 {
		return Blueprint{}
	}
	payload, err := json.Marshal(bp)
	if err != nil {
		return bp
	}
	var cloned Blueprint
	if err := json.Unmarshal(payload, &cloned); err != nil {
		return bp
	}
	return cloned
}

func notifyObserver(observer RunObserver, run *RunState) {
	if observer == nil || run == nil {
		return
	}
	observer.OnRunSnapshot(cloneRunState(run))
}

func syncSkippedNodeStates(run *RunState, scheduler *Scheduler, completedAt string) bool {
	if run == nil || scheduler == nil {
		return false
	}
	changed := false
	for nodeID := range scheduler.Skipped {
		state := run.NodeStates[nodeID]
		if state.Status != "" && state.Status != "pending" {
			continue
		}
		state.Status = "skipped"
		if state.CompletedAt == "" {
			state.CompletedAt = completedAt
		}
		run.NodeStates[nodeID] = state
		changed = true
	}
	return changed
}
