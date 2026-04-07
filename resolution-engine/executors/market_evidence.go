package executors

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

type MarketEvidenceConfig struct{}

type MarketEvidenceExecutor struct {
	IndexerURL string
	Client     *http.Client
}

type marketEvidenceResponse struct {
	AppID                  int             `json:"appId"`
	Question               string          `json:"question"`
	Outcomes               []string        `json:"outcomes"`
	SubmissionCount        int             `json:"submissionCount"`
	ClaimedOutcomeSummary  json.RawMessage `json:"claimedOutcomeSummary"`
	Entries                json.RawMessage `json:"entries"`
	ResolutionPendingSince int64           `json:"resolutionPendingSince"`
}

func NewMarketEvidenceExecutor(indexerURL string) *MarketEvidenceExecutor {
	return &MarketEvidenceExecutor{
		IndexerURL: strings.TrimRight(indexerURL, "/"),
		Client:     &http.Client{Timeout: 15 * time.Second},
	}
}

func (e *MarketEvidenceExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	if _, err := parseConfig[MarketEvidenceConfig](node.Config); err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("market_evidence config: %w", err)
	}

	appID, err := parseMarketAppID(execCtx)
	if err != nil {
		return dag.ExecutorResult{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/markets/%d/evidence", e.IndexerURL, appID), nil)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("create market_evidence request: %w", err)
	}

	resp, err := e.Client.Do(req)
	if err != nil {
		return dag.ExecutorResult{Outputs: map[string]string{
			"status": "failed",
			"error":  err.Error(),
		}}, nil
	}
	defer resp.Body.Close()

	var payload marketEvidenceResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return dag.ExecutorResult{Outputs: map[string]string{
			"status": "failed",
			"error":  fmt.Sprintf("decode market evidence response: %v", err),
		}}, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message := "market evidence request failed"
		return dag.ExecutorResult{Outputs: map[string]string{
			"status": "failed",
			"error":  message,
		}}, nil
	}

	entriesJSON := string(payload.Entries)
	if entriesJSON == "" || entriesJSON == "null" {
		entriesJSON = "[]"
	}
	claimedSummary := string(payload.ClaimedOutcomeSummary)
	if claimedSummary == "" || claimedSummary == "null" {
		claimedSummary = "{}"
	}

	rawBody, err := json.Marshal(payload)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("marshal market evidence payload: %w", err)
	}
	outcomesJSON, err := json.Marshal(payload.Outcomes)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("marshal market evidence outcomes: %w", err)
	}

	return dag.ExecutorResult{Outputs: map[string]string{
		"status":                   "success",
		"count":                    strconv.Itoa(payload.SubmissionCount),
		"entries_json":             entriesJSON,
		"claimed_summary":          claimedSummary,
		"raw":                      string(rawBody),
		"question":                 payload.Question,
		"outcomes_json":            string(outcomesJSON),
		"resolution_pending_since": strconv.FormatInt(payload.ResolutionPendingSince, 10),
	}}, nil
}
