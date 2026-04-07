package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

type TraceMetadata struct {
	AppID         int    `json:"appId"`
	BlueprintPath string `json:"blueprintPath"`
	Initiator     string `json:"initiator"`
}

type RunOptions struct {
	Trace *TraceMetadata
}

type TraceEnvelope struct {
	AppID         int           `json:"appId"`
	BlueprintPath string        `json:"blueprintPath"`
	Initiator     string        `json:"initiator"`
	Revision      int           `json:"revision"`
	Run           *dag.RunState `json:"run"`
}

type traceSink interface {
	Enqueue(TraceEnvelope) bool
}

type TraceEmitter struct {
	indexerURL string
	token      string
	client     *http.Client
	logger     *slog.Logger
	queue      chan TraceEnvelope
	done       chan struct{}
}

func NewTraceEmitter(indexerURL string, token string, logger *slog.Logger) *TraceEmitter {
	indexerURL = strings.TrimRight(strings.TrimSpace(indexerURL), "/")
	if indexerURL == "" {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}

	emitter := &TraceEmitter{
		indexerURL: indexerURL,
		token:      strings.TrimSpace(token),
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
		logger: logger,
		queue:  make(chan TraceEnvelope, 64),
		done:   make(chan struct{}),
	}

	go emitter.run()
	return emitter
}

func (e *TraceEmitter) Close() {
	if e == nil {
		return
	}
	select {
	case <-e.done:
		return
	default:
		close(e.done)
	}
}

func (e *TraceEmitter) Enqueue(envelope TraceEnvelope) bool {
	if e == nil {
		return false
	}
	select {
	case <-e.done:
		return false
	default:
	}

	select {
	case e.queue <- envelope:
		return true
	default:
		e.logger.Warn("trace emitter queue full; dropping snapshot",
			"component", "trace_emitter",
			"app_id", envelope.AppID,
			"run_id", runIDForLog(envelope.Run),
			"revision", envelope.Revision,
		)
		return false
	}
}

func (e *TraceEmitter) run() {
	for {
		select {
		case <-e.done:
			return
		case envelope := <-e.queue:
			if err := e.postSnapshot(envelope); err != nil {
				e.logger.Warn("trace emitter post failed",
					"component", "trace_emitter",
					"app_id", envelope.AppID,
					"run_id", runIDForLog(envelope.Run),
					"revision", envelope.Revision,
					"error", err,
				)
			}
		}
	}
}

func (e *TraceEmitter) postSnapshot(envelope TraceEnvelope) error {
	if envelope.Run == nil {
		return fmt.Errorf("trace run is required")
	}

	body, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal trace envelope: %w", err)
	}

	url := fmt.Sprintf("%s/markets/%d/blueprint-runs", e.indexerURL, envelope.AppID)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build trace ingest request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if e.token != "" {
		req.Header.Set("Authorization", "Bearer "+e.token)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("post trace snapshot: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	if len(bodyBytes) == 0 {
		return fmt.Errorf("trace ingest returned status %d", resp.StatusCode)
	}
	return fmt.Errorf("trace ingest returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
}

func runIDForLog(run *dag.RunState) string {
	if run == nil {
		return ""
	}
	return run.ID
}
