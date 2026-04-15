package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

type signalRequest struct {
	ID             string            `json:"id,omitempty"`
	IdempotencyKey string            `json:"idempotency_key"`
	AppID          int               `json:"app_id"`
	RunID          string            `json:"run_id,omitempty"`
	SignalType     string            `json:"signal_type"`
	CorrelationKey string            `json:"correlation_key"`
	ObservedAt     string            `json:"observed_at,omitempty"`
	Payload        map[string]string `json:"payload,omitempty"`
}

type signalResult struct {
	RunID  string    `json:"run_id"`
	AppID  int       `json:"app_id"`
	Status RunStatus `json:"status"`
}

type durableRunRecord struct {
	Request           RunRequest               `json:"request"`
	Result            RunResult                `json:"result"`
	Checkpoint        durableCheckpoint        `json:"checkpoint"`
	Signals           map[string]signalRequest `json:"signals,omitempty"`
	CallbackDelivered bool                     `json:"callback_delivered,omitempty"`
	CallbackAttempts  int                      `json:"callback_attempts,omitempty"`
	NextCallbackAt    string                   `json:"next_callback_at,omitempty"`
	CreatedAt         string                   `json:"created_at"`
	UpdatedAt         string                   `json:"updated_at"`
	CompletedAt       string                   `json:"completed_at,omitempty"`
}

type durableCheckpoint struct {
	Run            *dag.RunState                 `json:"run"`
	Context        map[string]string             `json:"context"`
	Completed      map[string]bool               `json:"completed,omitempty"`
	Failed         map[string]bool               `json:"failed,omitempty"`
	Activated      map[string]bool               `json:"activated,omitempty"`
	Skipped        map[string]bool               `json:"skipped,omitempty"`
	IterationCount map[string]int                `json:"iteration_count,omitempty"`
	EdgeTraversals map[string]int                `json:"edge_traversals,omitempty"`
	Waiting        map[string]durableWaitingNode `json:"waiting,omitempty"`
}

type durableWaitingNode struct {
	NodeID         string            `json:"node_id"`
	Kind           string            `json:"kind"`
	Reason         string            `json:"reason,omitempty"`
	SignalType     string            `json:"signal_type,omitempty"`
	CorrelationKey string            `json:"correlation_key,omitempty"`
	ResumeAtUnix   int64             `json:"resume_at_unix,omitempty"`
	Outputs        map[string]string `json:"outputs,omitempty"`
	TimeoutOutputs map[string]string `json:"timeout_outputs,omitempty"`
	StartedAt      string            `json:"started_at,omitempty"`
	InputSnapshot  map[string]string `json:"input_snapshot,omitempty"`
	Iteration      int               `json:"iteration,omitempty"`
}

type durableEvent struct {
	ID        string      `json:"id"`
	RunID     string      `json:"run_id"`
	Type      string      `json:"type"`
	CreatedAt string      `json:"created_at"`
	Payload   interface{} `json:"payload,omitempty"`
}

type durableFileStore struct {
	dir string
	mu  sync.Mutex
}

func newDurableFileStore(dir string) (*durableFileStore, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("durable store directory is required")
	}
	store := &durableFileStore{dir: dir}
	for _, subdir := range []string{"runs", "events"} {
		if err := os.MkdirAll(filepath.Join(dir, subdir), 0o755); err != nil {
			return nil, err
		}
	}
	return store, nil
}

func (s *durableFileStore) runPath(runID string) string {
	return filepath.Join(s.dir, "runs", runID+".json")
}

func (s *durableFileStore) eventPath(runID string) string {
	return filepath.Join(s.dir, "events", runID+".jsonl")
}

func (s *durableFileStore) saveRun(record *durableRunRecord) error {
	if record == nil {
		return errors.New("run record is required")
	}
	runID := strings.TrimSpace(record.Request.RunID)
	if runID == "" {
		return errors.New("run_id is required")
	}
	record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)

	s.mu.Lock()
	defer s.mu.Unlock()
	return writeJSONAtomic(s.runPath(runID), record)
}

func (s *durableFileStore) loadRun(runID string) (*durableRunRecord, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, errors.New("run_id is required")
	}
	data, err := os.ReadFile(s.runPath(runID))
	if err != nil {
		return nil, err
	}
	var record durableRunRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, err
	}
	ensureDurableRecordMaps(&record)
	return &record, nil
}

func (s *durableFileStore) listRuns() ([]*durableRunRecord, error) {
	entries, err := os.ReadDir(filepath.Join(s.dir, "runs"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	runs := make([]*durableRunRecord, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		runID := strings.TrimSuffix(entry.Name(), ".json")
		record, err := s.loadRun(runID)
		if err != nil {
			return nil, err
		}
		runs = append(runs, record)
	}
	return runs, nil
}

func (s *durableFileStore) appendEvent(runID string, eventType string, payload interface{}) error {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return errors.New("run_id is required")
	}
	event := durableEvent{
		ID:        fmt.Sprintf("%s:%d", runID, time.Now().UTC().UnixNano()),
		RunID:     runID,
		Type:      eventType,
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Payload:   payload,
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	file, err := os.OpenFile(s.eventPath(runID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(append(data, '\n')); err != nil {
		return err
	}
	return file.Sync()
}

func (s *durableFileStore) loadEvents(runID string) ([]durableEvent, error) {
	data, err := os.ReadFile(s.eventPath(runID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	events := make([]durableEvent, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var event durableEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func writeJSONAtomic(path string, value interface{}) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	cleanup = false
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

func ensureDurableRecordMaps(record *durableRunRecord) {
	if record == nil {
		return
	}
	if record.Signals == nil {
		record.Signals = make(map[string]signalRequest)
	}
	cp := &record.Checkpoint
	if cp.Context == nil {
		cp.Context = make(map[string]string)
	}
	if cp.Completed == nil {
		cp.Completed = make(map[string]bool)
	}
	if cp.Failed == nil {
		cp.Failed = make(map[string]bool)
	}
	if cp.Activated == nil {
		cp.Activated = make(map[string]bool)
	}
	if cp.Skipped == nil {
		cp.Skipped = make(map[string]bool)
	}
	if cp.IterationCount == nil {
		cp.IterationCount = make(map[string]int)
	}
	if cp.EdgeTraversals == nil {
		cp.EdgeTraversals = make(map[string]int)
	}
	if cp.Waiting == nil {
		cp.Waiting = make(map[string]durableWaitingNode)
	}
	if cp.Run != nil {
		if cp.Run.NodeStates == nil {
			cp.Run.NodeStates = make(map[string]dag.NodeState)
		}
		if cp.Run.EdgeTraversals == nil {
			cp.Run.EdgeTraversals = make(map[string]int)
		}
		if cp.Run.Context == nil {
			cp.Run.Context = make(map[string]string)
		}
	}
}
