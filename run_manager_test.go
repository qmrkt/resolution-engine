package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/qmrkt/resolution-engine/dag"
)

type fakeRunExecutor struct {
	mu       sync.Mutex
	run      *dag.RunState
	err      error
	block    bool
	runCount int
}

func (f *fakeRunExecutor) RunBlueprint(appID int, resolutionLogicJSON []byte, inputs map[string]string, opts ...RunOptions) (*dag.RunState, error) {
	f.mu.Lock()
	f.runCount++
	shouldBlock := f.block
	f.mu.Unlock()
	if shouldBlock {
		ctx := firstRunOptions(opts...).Context
		if ctx == nil {
			ctx = context.Background()
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return f.run, f.err
}

func (f *fakeRunExecutor) WriteEvidence(appID int, payload interface{}) (string, error) {
	return "evidence.json", nil
}

func (f *fakeRunExecutor) RunCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runCount
}

type blockingRunExecutor struct {
	started     chan int
	released    chan struct{}
	terminalRun *dag.RunState
}

func (b *blockingRunExecutor) RunBlueprint(appID int, resolutionLogicJSON []byte, inputs map[string]string, opts ...RunOptions) (*dag.RunState, error) {
	runOpts := firstRunOptions(opts...)
	ctx := runOpts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	select {
	case b.started <- appID:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case <-b.released:
		return b.terminalRun, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *blockingRunExecutor) WriteEvidence(appID int, payload interface{}) (string, error) {
	return "evidence.json", nil
}

func completedRun() *dag.RunState {
	return &dag.RunState{
		Status: "completed",
		Definition: dag.Blueprint{
			ID: "bp",
			Nodes: []dag.NodeDef{
				{
					ID:     "submit",
					Type:   "submit_result",
					Config: map[string]interface{}{"outcome_key": "judge.outcome"},
				},
			},
			Edges: []dag.EdgeDef{
				{From: "judge", To: "submit"},
			},
			Inputs: []dag.InputDef{
				{Name: "market_app_id", Required: true},
			},
			Budget: &dag.Budget{
				MaxTotalTimeSeconds: 60,
				PerNode: map[string]dag.NodeBud{
					"submit": dag.NodeBud{MaxTimeSeconds: 10},
				},
			},
		},
		Inputs: map[string]string{
			"market_app_id": "11",
		},
		NodeStates: map[string]dag.NodeState{
			"submit": dag.NodeState{Status: "completed"},
		},
		Context: map[string]string{
			"submit.outcome":       "1",
			"submit.evidence_hash": "abc",
			"submit.submitted":     "true",
		},
		EdgeTraversals: map[string]int{
			"judge->submit": 1,
		},
		NodeTraces: []dag.NodeTrace{
			{
				NodeID:        "submit",
				Status:        "completed",
				InputSnapshot: map[string]string{"judge.outcome": "1"},
				Outputs:       map[string]string{"outcome": "1"},
			},
		},
	}
}

func waitForRunStatus(t *testing.T, manager *RunManager, runID string, status RunStatus) RunResult {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	var last RunResult
	found := false
	for time.Now().Before(deadline) {
		result, ok := manager.Get(runID)
		if ok {
			found = true
			last = result
			if result.Status == status {
				return result
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !found {
		t.Fatalf("timed out waiting for run %q to exist", runID)
	}
	t.Fatalf("timed out waiting for run %q status %q; last status %q result %+v", runID, status, last.Status, last)
	return RunResult{}
}

func TestRunManagerSubmitAndCompleteLifecycle(t *testing.T) {
	exec := &fakeRunExecutor{run: completedRun()}
	manager := NewRunManagerWithConfig(exec, nil, "", time.Hour, time.Hour)
	defer manager.Close()

	accepted, err := manager.Submit(RunRequest{RunID: "run-1", AppID: 11, BlueprintJSON: []byte(`{"id":"bp"}`), BlueprintPath: PathMain})
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Status != RunStatusAccepted {
		t.Fatalf("accepted status = %q, want %q", accepted.Status, RunStatusAccepted)
	}

	result := waitForRunStatus(t, manager, "run-1", RunStatusCompleted)
	if result.Action != RunActionPropose {
		t.Fatalf("action = %q, want %q", result.Action, RunActionPropose)
	}
	if result.Outcome != "1" || result.EvidenceHash != "abc" {
		t.Fatalf("unexpected terminal result: %+v", result)
	}
}

func TestRunManagerSubmitReturnsBeforeExecutionCompletes(t *testing.T) {
	exec := &blockingRunExecutor{
		started:     make(chan int, 1),
		released:    make(chan struct{}),
		terminalRun: completedRun(),
	}
	manager := NewRunManagerWithConfig(exec, nil, "", time.Hour, time.Hour)
	defer manager.Close()

	submitted := make(chan struct{})
	var accepted RunResult
	var submitErr error
	go func() {
		accepted, submitErr = manager.Submit(RunRequest{RunID: "run-async", AppID: 15, BlueprintJSON: []byte(`{"id":"bp"}`)})
		close(submitted)
	}()

	select {
	case <-submitted:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("submit blocked behind blueprint execution")
	}
	if submitErr != nil {
		t.Fatal(submitErr)
	}
	if accepted.Status != RunStatusAccepted {
		t.Fatalf("accepted status = %q, want %q", accepted.Status, RunStatusAccepted)
	}

	select {
	case appID := <-exec.started:
		if appID != 15 {
			t.Fatalf("executor app_id = %d, want 15", appID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for executor to start")
	}
	waitForRunStatus(t, manager, "run-async", RunStatusRunning)

	close(exec.released)
	waitForRunStatus(t, manager, "run-async", RunStatusCompleted)
}

func TestRunManagerRunsDifferentAppsConcurrently(t *testing.T) {
	exec := &blockingRunExecutor{
		started:     make(chan int, 2),
		released:    make(chan struct{}),
		terminalRun: completedRun(),
	}
	manager := NewRunManagerWithConfig(exec, nil, "", time.Hour, time.Hour)
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "run-a", AppID: 21, BlueprintJSON: []byte(`{"id":"bp"}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Submit(RunRequest{RunID: "run-b", AppID: 22, BlueprintJSON: []byte(`{"id":"bp"}`)}); err != nil {
		t.Fatal(err)
	}

	seen := map[int]bool{}
	for len(seen) < 2 {
		select {
		case appID := <-exec.started:
			seen[appID] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for both runs to start; seen=%v", seen)
		}
	}
	if !seen[21] || !seen[22] {
		t.Fatalf("expected app IDs 21 and 22 to run, seen=%v", seen)
	}
	if active := manager.ActiveCount(); active != 2 {
		t.Fatalf("active count = %d, want 2", active)
	}

	close(exec.released)
	waitForRunStatus(t, manager, "run-a", RunStatusCompleted)
	waitForRunStatus(t, manager, "run-b", RunStatusCompleted)
}

func TestRunManagerRejectsDuplicateInflightAppID(t *testing.T) {
	exec := &fakeRunExecutor{block: true}
	manager := NewRunManager(exec, nil, "")
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "run-a", AppID: 12, BlueprintJSON: []byte(`{"id":"bp"}`)}); err != nil {
		t.Fatal(err)
	}
	_, err := manager.Submit(RunRequest{RunID: "run-b", AppID: 12, BlueprintJSON: []byte(`{"id":"bp"}`)})
	var dup *duplicateRunError
	if !errors.As(err, &dup) {
		t.Fatalf("expected duplicateRunError, got %v", err)
	}
	if dup.RunID != "run-a" {
		t.Fatalf("duplicate run id = %q, want run-a", dup.RunID)
	}
}

func TestRunManagerAllowsSameAppAfterTerminalRun(t *testing.T) {
	exec := &fakeRunExecutor{run: completedRun()}
	manager := NewRunManagerWithConfig(exec, nil, "", time.Hour, time.Hour)
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "run-first", AppID: 23, BlueprintJSON: []byte(`{"id":"bp"}`)}); err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, manager, "run-first", RunStatusCompleted)

	if _, err := manager.Submit(RunRequest{RunID: "run-second", AppID: 23, BlueprintJSON: []byte(`{"id":"bp"}`)}); err != nil {
		t.Fatalf("expected same app_id to be reusable after terminal run, got %v", err)
	}
	waitForRunStatus(t, manager, "run-second", RunStatusCompleted)
	if count := exec.RunCount(); count != 2 {
		t.Fatalf("run count = %d, want 2", count)
	}
}

func TestRunManagerCancelTransitionsToCancelled(t *testing.T) {
	exec := &fakeRunExecutor{block: true}
	manager := NewRunManager(exec, nil, "")
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "run-cancel", AppID: 13, BlueprintJSON: []byte(`{"id":"bp"}`)}); err != nil {
		t.Fatal(err)
	}
	if _, ok := manager.Cancel("run-cancel"); !ok {
		t.Fatal("expected cancel to find run")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		result, ok := manager.Get("run-cancel")
		if ok && result.Status == RunStatusCancelled {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for cancelled status")
}

func TestRunManagerCleansUpExpiredTerminalRuns(t *testing.T) {
	exec := &fakeRunExecutor{run: completedRun()}
	manager := NewRunManagerWithConfig(exec, nil, "", 10*time.Millisecond, time.Hour)
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "run-expire", AppID: 14, BlueprintJSON: []byte(`{"id":"bp"}`)}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	manager.cleanupExpired()
	if _, ok := manager.Get("run-expire"); ok {
		t.Fatal("expected terminal run to be cleaned up")
	}
}

func TestRunManagerGetReturnsDefensiveRunStateCopy(t *testing.T) {
	exec := &fakeRunExecutor{run: completedRun()}
	manager := NewRunManagerWithConfig(exec, nil, "", time.Hour, time.Hour)
	defer manager.Close()

	if _, err := manager.Submit(RunRequest{RunID: "run-copy", AppID: 24, BlueprintJSON: []byte(`{"id":"bp"}`)}); err != nil {
		t.Fatal(err)
	}
	result := waitForRunStatus(t, manager, "run-copy", RunStatusCompleted)
	if result.RunState == nil {
		t.Fatal("expected run state")
	}

	result.RunState.Inputs["market_app_id"] = "mutated"
	result.RunState.Context["submit.outcome"] = "mutated"
	result.RunState.EdgeTraversals["judge->submit"] = 99
	result.RunState.NodeStates["submit"] = dag.NodeState{Status: "mutated"}
	result.RunState.NodeTraces[0].InputSnapshot["judge.outcome"] = "mutated"
	result.RunState.NodeTraces[0].Outputs["outcome"] = "mutated"
	result.RunState.Definition.Nodes[0].ID = "mutated"
	result.RunState.Definition.Nodes[0].Config.(map[string]interface{})["outcome_key"] = "mutated"
	result.RunState.Definition.Edges[0].To = "mutated"
	result.RunState.Definition.Inputs[0].Name = "mutated"
	result.RunState.Definition.Budget.PerNode["submit"] = dag.NodeBud{MaxTimeSeconds: 99}

	fresh, ok := manager.Get("run-copy")
	if !ok {
		t.Fatal("expected run-copy to remain available")
	}
	if fresh.RunState.Inputs["market_app_id"] != "11" {
		t.Fatalf("input leaked mutation: %+v", fresh.RunState.Inputs)
	}
	if fresh.RunState.Context["submit.outcome"] != "1" {
		t.Fatalf("context leaked mutation: %+v", fresh.RunState.Context)
	}
	if fresh.RunState.EdgeTraversals["judge->submit"] != 1 {
		t.Fatalf("edge traversals leaked mutation: %+v", fresh.RunState.EdgeTraversals)
	}
	if fresh.RunState.NodeStates["submit"].Status != "completed" {
		t.Fatalf("node states leaked mutation: %+v", fresh.RunState.NodeStates)
	}
	if fresh.RunState.NodeTraces[0].InputSnapshot["judge.outcome"] != "1" {
		t.Fatalf("trace input snapshot leaked mutation: %+v", fresh.RunState.NodeTraces[0].InputSnapshot)
	}
	if fresh.RunState.NodeTraces[0].Outputs["outcome"] != "1" {
		t.Fatalf("trace outputs leaked mutation: %+v", fresh.RunState.NodeTraces[0].Outputs)
	}
	if fresh.RunState.Definition.Nodes[0].ID != "submit" {
		t.Fatalf("definition nodes leaked mutation: %+v", fresh.RunState.Definition.Nodes)
	}
	if fresh.RunState.Definition.Nodes[0].Config.(map[string]interface{})["outcome_key"] != "judge.outcome" {
		t.Fatalf("definition node config leaked mutation: %+v", fresh.RunState.Definition.Nodes[0].Config)
	}
	if fresh.RunState.Definition.Edges[0].To != "submit" {
		t.Fatalf("definition edges leaked mutation: %+v", fresh.RunState.Definition.Edges)
	}
	if fresh.RunState.Definition.Inputs[0].Name != "market_app_id" {
		t.Fatalf("definition inputs leaked mutation: %+v", fresh.RunState.Definition.Inputs)
	}
	if fresh.RunState.Definition.Budget.PerNode["submit"].MaxTimeSeconds != 10 {
		t.Fatalf("definition budget leaked mutation: %+v", fresh.RunState.Definition.Budget.PerNode)
	}
}
