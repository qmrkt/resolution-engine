package dag

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Executor executes a single node in the DAG. Executors read run identity,
// caller inputs, and prior-node outputs from the Invocation; returned
// Outputs are written into Invocation.Results by the engine.
type Executor interface {
	Execute(ctx context.Context, node NodeDef, inv *Invocation) (ExecutorResult, error)
}

// TerminalExecutor is an optional extension marking executors whose
// EarlyReturn a blueprint MUST produce for the run to succeed. If a
// blueprint contains a node whose registered executor implements
// TerminalExecutor and IsTerminal() returns true, but no such node's
// EarlyReturn fires during the run, the engine marks the run failed
// with "blueprint completed without firing return". The engine itself
// remains ignorant of specific node type names.
type TerminalExecutor interface {
	IsTerminal() bool
}

// CancellableExecutor is an optional extension for executors that track
// in-flight work keyed by a correlation string (e.g. async agent loops).
// The engine's CancelCorrelation walks every registered executor that
// implements this interface.
type CancellableExecutor interface {
	CancelCorrelation(correlationKey string) bool
}

// SuspendableExecutor is an optional extension declared by executors that may
// return a non-nil dag.Suspension. Validators for inline sub-blueprints
// (map bodies, gadget children, agent blueprint tools) call
// Engine.ValidateNoSuspensionCapableNodes to reject such nodes — those
// contexts drive the in-memory engine, which has no waiting state.
// CanSuspend may read node.Config to decide per-instance (e.g. agent_loop is
// only suspension-capable when Async is set).
type SuspendableExecutor interface {
	Executor
	CanSuspend(node NodeDef) bool
}

// ConfigSchemaProvider declares the JSON Schema for an executor's Config
// field. Consumed by the node catalog and by tools that want to verify a
// node's config shape before submission. The returned schema must be a
// valid JSON Schema draft-07 object.
type ConfigSchemaProvider interface {
	ConfigSchema() json.RawMessage
}

// OutputKeyProvider enumerates the output keys an executor writes into the
// shared context under its node ID. The list is advisory — executors may
// emit additional keys under certain configs (agent_loop output_mode,
// cel_eval expressions) — so callers should treat it as the canonical set,
// not an exhaustive one. Consumed by the node catalog and by edge-condition
// diagnostics ("did you mean X?") in blueprint validation.
type OutputKeyProvider interface {
	OutputKeys() []string
}

// RunObserver receives defensive copies of a run as execution progresses.
type RunObserver interface {
	OnRunSnapshot(run *RunState)
}

// Engine runs resolution logic DAGs with registered executors.
type Engine struct {
	mu        sync.RWMutex
	executors map[string]Executor
	logger    *slog.Logger
}

// NewEngine creates a DAG execution engine.
func NewEngine(logger *slog.Logger) *Engine {
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		executors: make(map[string]Executor),
		logger:    logger,
	}
}

// RegisterExecutor registers an executor for a node type.
func (e *Engine) RegisterExecutor(nodeType string, exec Executor) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.executors[nodeType] = exec
}

func (e *Engine) getExecutor(nodeType string) (Executor, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	exec, ok := e.executors[nodeType]
	return exec, ok
}

// ExecutorFor returns the registered executor for a node type.
func (e *Engine) ExecutorFor(nodeType string) (Executor, bool) {
	return e.getExecutor(nodeType)
}

// CancelCorrelation asks every registered CancellableExecutor to cancel
// work tied to correlationKey and returns true if any reported a hit.
// Safe to call with unknown keys.
func (e *Engine) CancelCorrelation(correlationKey string) bool {
	if e == nil || correlationKey == "" {
		return false
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	cancelled := false
	for _, exec := range e.executors {
		if c, ok := exec.(CancellableExecutor); ok {
			if c.CancelCorrelation(correlationKey) {
				cancelled = true
			}
		}
	}
	return cancelled
}

// CanSuspend returns true when the registered executor for node.Type both
// implements SuspendableExecutor and reports CanSuspend(node) = true.
// Unknown node types and executors that don't implement the interface are
// treated as non-suspending.
func (e *Engine) CanSuspend(node NodeDef) bool {
	if e == nil {
		return false
	}
	exec, ok := e.getExecutor(node.Type)
	if !ok {
		return false
	}
	s, ok := exec.(SuspendableExecutor)
	if !ok {
		return false
	}
	return s.CanSuspend(node)
}

// ConfigSchemaFor returns the JSON Schema the executor for nodeType declares
// via ConfigSchemaProvider, or nil when the executor is unregistered or
// doesn't implement the interface.
func (e *Engine) ConfigSchemaFor(nodeType string) json.RawMessage {
	if e == nil {
		return nil
	}
	exec, ok := e.getExecutor(nodeType)
	if !ok {
		return nil
	}
	p, ok := exec.(ConfigSchemaProvider)
	if !ok {
		return nil
	}
	return p.ConfigSchema()
}

// OutputKeysFor returns the output-key list the executor for nodeType
// declares via OutputKeyProvider, or nil when the executor is unregistered
// or doesn't implement the interface.
func (e *Engine) OutputKeysFor(nodeType string) []string {
	if e == nil {
		return nil
	}
	exec, ok := e.getExecutor(nodeType)
	if !ok {
		return nil
	}
	p, ok := exec.(OutputKeyProvider)
	if !ok {
		return nil
	}
	return p.OutputKeys()
}

// ValidateNoSuspensionCapableNodes fails when any node in bp could, under
// the durable engine, return a non-nil Suspension. Call this at the
// boundary of every sub-blueprint entry point that dispatches through the
// in-memory engine (gadget children, map inline bodies, agent blueprint
// tools); top-level durable runs do not need the check.
func (e *Engine) ValidateNoSuspensionCapableNodes(bp Blueprint) error {
	if e == nil {
		return nil
	}
	offenders := make([]string, 0)
	for _, node := range bp.Nodes {
		if e.CanSuspend(node) {
			offenders = append(offenders, fmt.Sprintf("%s (%s)", node.ID, node.Type))
		}
	}
	if len(offenders) == 0 {
		return nil
	}
	sort.Strings(offenders)
	return fmt.Errorf(
		"inline sub-blueprint contains suspension-capable node(s) not permitted outside the durable engine: %s",
		strings.Join(offenders, ", "),
	)
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

	ctx, cancelExecution := context.WithCancel(ctx)
	defer cancelExecution()

	now := time.Now().UTC()
	runID := uuid.New().String()
	frozenInputs := cloneStringMap(inputs)
	if frozenInputs == nil {
		frozenInputs = map[string]string{}
	}
	inv := NewInvocation(Run{
		ID:          runID,
		BlueprintID: bp.ID,
		StartedAt:   now,
		Inputs:      frozenInputs,
	})
	run := &RunState{
		ID:             runID,
		BlueprintID:    bp.ID,
		Definition:     bp,
		Status:         "running",
		Inputs:         cloneStringMap(frozenInputs),
		NodeStates:     make(map[string]NodeState, len(bp.Nodes)),
		Results:        inv.Results,
		EdgeTraversals: make(map[string]int),
		StartedAt:      now.Format(time.RFC3339),
	}

	for _, node := range bp.Nodes {
		run.NodeStates[node.ID] = NodeState{Status: "pending"}
	}
	notifyObserver(observer, run)

	err := e.runStateMachine(ctx, bp, run, inv, observer, cancelExecution)

	run.CompletedAt = time.Now().UTC().Format(time.RFC3339)

	if err != nil {
		run.Status = "failed"
		run.Error = err.Error()
		notifyObserver(observer, run)
		return run, err
	}
	if len(run.Return) > 0 {
		run.Status = "completed"
		run.Error = ""
		notifyObserver(observer, run)
		return run, nil
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
		if e.BlueprintRequiresTerminalFire(bp) {
			run.Status = "failed"
			run.Error = "blueprint completed without firing return"
			notifyObserver(observer, run)
			return run, errors.New(run.Error)
		}
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
	inv *Invocation,
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
	var earlyReturnValue *json.RawMessage
	var earlyReturnNodeID string
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
		if earlyReturnValue != nil {
			break
		}
		if err := ctx.Err(); err != nil {
			run.EdgeTraversals = scheduler.TraversalSnapshot()
			if syncSkippedNodeStates(run, scheduler, time.Now().UTC().Format(time.RFC3339)) {
				notifyObserver(observer, run)
			}
			return fmt.Errorf("execution cancelled: %w", err)
		}

		ready := scheduler.ReadyNodes(completed, failed, running)
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
			inputSnapshots[nodeID] = FlattenInvocation(inv)
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
				result, err := exec.Execute(ctx, node, inv)
				if err == nil && result.Suspend != nil {
					// The in-memory engine has no waiting state or resume
					// machinery; a Suspend here means the executor expected
					// to be run under a durable engine. Fail loudly so the
					// misconfiguration surfaces in tests and callers.
					err = fmt.Errorf(
						"node %q requested suspension (kind=%q), which is not supported by the in-memory engine",
						nid, result.Suspend.Kind,
					)
					result = ExecutorResult{}
				}
				resultCh <- nodeResult{nodeID: nid, result: result, err: err}
			}(nodeID)
		}
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

			if earlyReturnValue != nil && nr.nodeID != earlyReturnNodeID {
				state.Status = "skipped"
				state.CompletedAt = completedAt
				state.Error = ""
				run.NodeStates[nr.nodeID] = state
				trace.Status = "skipped"
				trace.Usage = nr.result.Usage
				run.NodeTraces = append(run.NodeTraces, trace)
				delete(inputSnapshots, nr.nodeID)
				delete(startedAt, nr.nodeID)
				notifyObserver(observer, run)
				continue
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

				inv.Results.SetField(nr.nodeID, "status", "failed")
				inv.Results.SetField(nr.nodeID, "error", nr.err.Error())

				if node.OnError != "continue" {
					failed[nr.nodeID] = struct{}{}
				} else {
					completed[nr.nodeID] = struct{}{}
				}
			} else {
				nextInputTokens := run.Usage.InputTokens + nr.result.Usage.InputTokens
				nextOutputTokens := run.Usage.OutputTokens + nr.result.Usage.OutputTokens
				state.CompletedAt = completedAt
				state.Usage = nr.result.Usage

				run.Usage.InputTokens = nextInputTokens
				run.Usage.OutputTokens = nextOutputTokens

				if nr.result.EarlyReturn != nil {
					inv.Results.MergeOutputs(nr.nodeID, nr.result.Outputs)
					state.Status = "completed"
					state.Error = ""
					run.NodeStates[nr.nodeID] = state
					trace.Status = "completed"
					trace.Outputs = cloneStringMap(nr.result.Outputs)
					trace.Usage = nr.result.Usage
					run.NodeTraces = append(run.NodeTraces, trace)
					if status, _ := inv.Results.Get(nr.nodeID, "status"); status == "" {
						inv.Results.SetField(nr.nodeID, "status", "completed")
					}
					completed[nr.nodeID] = struct{}{}
					earlyReturnValue = cloneRawMessagePtr(nr.result.EarlyReturn)
					earlyReturnNodeID = nr.nodeID
					budgetErr = nil
					if cancelExecution != nil {
						cancelExecution()
					}
				} else {
					nextTotalTokens := nextInputTokens + nextOutputTokens
					tokenBudgetExceeded := bp.Budget != nil &&
						bp.Budget.MaxTotalTokens > 0 &&
						nextTotalTokens > bp.Budget.MaxTotalTokens

					inv.Results.MergeOutputs(nr.nodeID, nr.result.Outputs)

					if tokenBudgetExceeded && earlyReturnValue == nil {
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
						inv.Results.SetField(nr.nodeID, "status", "failed")
						inv.Results.SetField(nr.nodeID, "error", budgetErr.Error())
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
						if status, _ := inv.Results.Get(nr.nodeID, "status"); status == "" {
							inv.Results.SetField(nr.nodeID, "status", "completed")
						}

						completed[nr.nodeID] = struct{}{}
					}
				}
			}
			delete(inputSnapshots, nr.nodeID)
			delete(startedAt, nr.nodeID)
			if budgetErr != nil {
				notifyObserver(observer, run)
				continue
			}

			// Evaluate outgoing edges and activate targets
			edges, evalErr := scheduler.EvaluateEdges(nr.nodeID, inv)
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
				activated[edge.To] = struct{}{}

				// If target was already completed (back-edge), snapshot its
				// outputs into _runs history before resetting.
				if _, wasCompleted := completed[edge.To]; wasCompleted {
					inv.Results.AppendHistory(edge.To)
					inv.Results.Reset(edge.To)
					delete(completed, edge.To)
					state := run.NodeStates[edge.To]
					state.Status = "pending"
					state.CompletedAt = ""
					state.Error = ""
					state.Usage = TokenUsage{}
					run.NodeStates[edge.To] = state
				}
			}
			if len(edges) > 0 {
				run.EdgeTraversals = scheduler.TraversalSnapshot()
			}
			notifyObserver(observer, run)
		}

		if budgetErr != nil && earlyReturnValue == nil {
			break
		}
	}
	run.EdgeTraversals = scheduler.TraversalSnapshot()
	if syncSkippedNodeStates(run, scheduler, time.Now().UTC().Format(time.RFC3339)) {
		notifyObserver(observer, run)
	}
	if earlyReturnValue != nil {
		run.Return = cloneRawMessage(*earlyReturnValue)
		if markActivatedPendingNodeStatesSkipped(run, activated, time.Now().UTC().Format(time.RFC3339)) {
			notifyObserver(observer, run)
		}
		return nil
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

func cloneRawMessage(value json.RawMessage) json.RawMessage {
	if len(value) == 0 {
		return nil
	}
	return append(json.RawMessage(nil), value...)
}

func cloneRawMessagePtr(value *json.RawMessage) *json.RawMessage {
	if value == nil {
		return nil
	}
	cloned := cloneRawMessage(*value)
	return &cloned
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
	cloned.Return = cloneRawMessage(run.Return)
	cloned.Inputs = cloneStringMap(run.Inputs)
	if run.Results != nil {
		results := NewResults()
		results.LoadSnapshot(run.Results.Snapshot())
		cloned.Results = results
	}
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
	cloned := bp
	if bp.Nodes != nil {
		cloned.Nodes = append([]NodeDef(nil), bp.Nodes...)
		for i := range cloned.Nodes {
			cloned.Nodes[i].Config = cloneConfigValue(cloned.Nodes[i].Config)
		}
	}
	if bp.Edges != nil {
		cloned.Edges = append([]EdgeDef(nil), bp.Edges...)
	}
	if bp.Inputs != nil {
		cloned.Inputs = append([]InputDef(nil), bp.Inputs...)
	}
	if bp.Budget != nil {
		budget := *bp.Budget
		if bp.Budget.PerNode != nil {
			budget.PerNode = make(map[string]NodeBud, len(bp.Budget.PerNode))
			for key, value := range bp.Budget.PerNode {
				budget.PerNode[key] = value
			}
		}
		cloned.Budget = &budget
	}
	return cloned
}

// cloneConfigValue deep-copies a node's free-form Config payload. Configs
// arrive as any (typically map[string]any decoded from JSON), so a JSON
// roundtrip is the cheapest deep-copy that handles arbitrary nesting.
func cloneConfigValue(value any) any {
	if value == nil {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var cloned any
	if err := json.Unmarshal(data, &cloned); err != nil {
		return value
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

func markActivatedPendingNodeStatesSkipped(run *RunState, activated map[string]struct{}, completedAt string) bool {
	if run == nil || len(activated) == 0 {
		return false
	}
	changed := false
	for nodeID := range activated {
		state := run.NodeStates[nodeID]
		if state.Status != "pending" {
			continue
		}
		state.Status = "skipped"
		state.CompletedAt = completedAt
		state.Error = ""
		run.NodeStates[nodeID] = state
		changed = true
	}
	return changed
}

// BlueprintRequiresTerminalFire reports whether the blueprint contains any
// node whose registered executor implements TerminalExecutor and declares
// itself terminal. Runs containing such a node are required to fire it via
// EarlyReturn; otherwise the engine marks the run failed. Replaces the
// old `BlueprintHasReturn(bp) { node.Type == "return" }` check, removing
// the domain-specific node-type string from the generic engine layer.
func (e *Engine) BlueprintRequiresTerminalFire(bp Blueprint) bool {
	if e == nil {
		return false
	}
	for _, node := range bp.Nodes {
		exec, ok := e.getExecutor(node.Type)
		if !ok {
			continue
		}
		if t, ok := exec.(TerminalExecutor); ok && t.IsTerminal() {
			return true
		}
	}
	return false
}
