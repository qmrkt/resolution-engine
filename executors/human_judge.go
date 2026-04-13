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

type HumanJudgeConfig struct {
	Prompt             string   `json:"prompt"`
	AllowedResponders  []string `json:"allowed_responders"`
	DesignatedAddress  string   `json:"designated_address,omitempty"`
	TimeoutSeconds     int      `json:"timeout_seconds"`
	RequireReason      bool     `json:"require_reason,omitempty"`
	AllowCancel        bool     `json:"allow_cancel,omitempty"`
}

type HumanJudgeExecutor struct {
	IndexerURL   string
	Client       *http.Client
	PollInterval time.Duration
}

type humanJudgmentRecord struct {
	JudgmentID          string   `json:"judgmentId"`
	AppID               int      `json:"appId"`
	RunID               string   `json:"runId"`
	NodeID              string   `json:"nodeId"`
	Status              string   `json:"status"`
	Prompt              string   `json:"prompt"`
	AllowedResponders   []string `json:"allowedResponders"`
	DesignatedAddress   string   `json:"designatedAddress,omitempty"`
	ResponseNonce       string   `json:"responseNonce"`
	TimeoutAt           int64    `json:"timeoutAt"`
	ResponseOutcome     int      `json:"responseOutcome"`
	ResponseReason      string   `json:"responseReason"`
	ResponderAddress    string   `json:"responderAddress"`
	ResponderRole       string   `json:"responderRole"`
	ResponseHash        string   `json:"responseHash"`
	ResponseSubmittedAt int64    `json:"responseSubmittedAt"`
}

func NewHumanJudgeExecutor(indexerURL string) *HumanJudgeExecutor {
	return &HumanJudgeExecutor{
		IndexerURL:   strings.TrimRight(indexerURL, "/"),
		Client:       &http.Client{Timeout: 15 * time.Second},
		PollInterval: 5 * time.Second,
	}
}

func (e *HumanJudgeExecutor) Execute(ctx context.Context, node dag.NodeDef, execCtx *dag.Context) (dag.ExecutorResult, error) {
	cfg, err := parseConfig[HumanJudgeConfig](node.Config)
	if err != nil {
		return dag.ExecutorResult{}, fmt.Errorf("human_judge config: %w", err)
	}

	prompt := strings.TrimSpace(execCtx.Interpolate(cfg.Prompt))
	if prompt == "" {
		return dag.ExecutorResult{}, fmt.Errorf("human_judge prompt is required")
	}
	if len(cfg.AllowedResponders) == 0 {
		return dag.ExecutorResult{}, fmt.Errorf("human_judge allowed_responders is required")
	}
	if cfg.TimeoutSeconds <= 0 {
		cfg.TimeoutSeconds = 172800
	}

	appID, err := parseMarketAppID(execCtx)
	if err != nil {
		return dag.ExecutorResult{}, err
	}
	runID, err := parseResolutionRunID(execCtx)
	if err != nil {
		return dag.ExecutorResult{}, err
	}

	judgmentID := fmt.Sprintf("%d:%s:%s", appID, runID, node.ID)
	judgment, err := e.findJudgment(ctx, appID, judgmentID)
	if err != nil {
		return dag.ExecutorResult{}, err
	}

	if judgment == nil {
		timeoutAt := time.Now().Add(time.Duration(cfg.TimeoutSeconds) * time.Second).UnixMilli()
		judgment, err = e.createJudgment(ctx, appID, humanJudgmentRecord{
			JudgmentID:        judgmentID,
			AppID:             appID,
			RunID:             runID,
			NodeID:            node.ID,
			Status:            "pending",
			Prompt:            prompt,
			AllowedResponders: cfg.AllowedResponders,
			DesignatedAddress: cfg.DesignatedAddress,
			TimeoutAt:         timeoutAt,
		}, cfg.RequireReason, cfg.AllowCancel)
		if err != nil {
			return dag.ExecutorResult{}, err
		}
	}

	pollInterval := e.PollInterval
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}

	for {
		outputs, done := humanJudgeOutputs(judgment)
		if done {
			return dag.ExecutorResult{Outputs: outputs}, nil
		}

		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return dag.ExecutorResult{}, ctx.Err()
		case <-timer.C:
		}

		judgment, err = e.findJudgment(ctx, appID, judgmentID)
		if err != nil {
			return dag.ExecutorResult{}, err
		}
		if judgment == nil {
			return dag.ExecutorResult{}, fmt.Errorf("human_judge %q disappeared while waiting", judgmentID)
		}
	}
}

func parseMarketAppID(execCtx *dag.Context) (int, error) {
	rawAppID := strings.TrimSpace(execCtx.Get("market_app_id"))
	if rawAppID == "" {
		return 0, fmt.Errorf("market_app_id is required in execution context")
	}
	appID, err := strconv.Atoi(rawAppID)
	if err != nil || appID <= 0 {
		return 0, fmt.Errorf("invalid market_app_id %q", rawAppID)
	}
	return appID, nil
}

func parseResolutionRunID(execCtx *dag.Context) (string, error) {
	runID := strings.TrimSpace(execCtx.Get("resolution_run_id"))
	if runID == "" {
		return "", fmt.Errorf("resolution_run_id is required in execution context")
	}
	return runID, nil
}

func humanJudgeOutputs(judgment *humanJudgmentRecord) (map[string]string, bool) {
	if judgment == nil {
		return nil, false
	}

	switch judgment.Status {
	case "responded":
		outcome := ""
		if judgment.ResponseOutcome >= 0 {
			outcome = strconv.Itoa(judgment.ResponseOutcome)
		}
		return map[string]string{
			"status":             "responded",
			"outcome":            outcome,
			"reason":             judgment.ResponseReason,
			"responder_address":  judgment.ResponderAddress,
			"responder_role":     judgment.ResponderRole,
			"response_hash":      judgment.ResponseHash,
			"response_submitted": strconv.FormatInt(judgment.ResponseSubmittedAt, 10),
		}, true
	case "cancelled":
		return map[string]string{
			"status":             "cancelled",
			"reason":             judgment.ResponseReason,
			"responder_address":  judgment.ResponderAddress,
			"responder_role":     judgment.ResponderRole,
			"response_hash":      judgment.ResponseHash,
			"response_submitted": strconv.FormatInt(judgment.ResponseSubmittedAt, 10),
		}, true
	case "timeout":
		return map[string]string{
			"status": "timeout",
		}, true
	default:
		return nil, false
	}
}

func (e *HumanJudgeExecutor) createJudgment(
	ctx context.Context,
	appID int,
	judgment humanJudgmentRecord,
	requireReason bool,
	allowCancel bool,
) (*humanJudgmentRecord, error) {
	payload := map[string]any{
		"judgmentId":        judgment.JudgmentID,
		"runId":             judgment.RunID,
		"nodeId":            judgment.NodeID,
		"prompt":            judgment.Prompt,
		"allowedResponders": judgment.AllowedResponders,
		"timeoutAt":         judgment.TimeoutAt,
		"requireReason":     requireReason,
		"allowCancel":       allowCancel,
	}
	if judgment.DesignatedAddress != "" {
		payload["designatedAddress"] = judgment.DesignatedAddress
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal human_judge payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/markets/%d/human-judgments", e.IndexerURL, appID), strings.NewReader(string(body)))
	if err != nil {
		return nil, fmt.Errorf("create human_judge request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("create human_judge: %w", err)
	}
	defer resp.Body.Close()

	var payloadResp struct {
		Judgment *humanJudgmentRecord `json:"judgment"`
		Error    string               `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payloadResp); err != nil {
		return nil, fmt.Errorf("decode human_judge create response: %w", err)
	}
	if resp.StatusCode >= 300 {
		if payloadResp.Error == "" {
			payloadResp.Error = resp.Status
		}
		return nil, fmt.Errorf("create human_judge: %s", payloadResp.Error)
	}
	if payloadResp.Judgment == nil {
		return nil, fmt.Errorf("create human_judge: missing judgment payload")
	}
	return payloadResp.Judgment, nil
}

func (e *HumanJudgeExecutor) findJudgment(ctx context.Context, appID int, judgmentID string) (*humanJudgmentRecord, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/markets/%d/human-judgments", e.IndexerURL, appID), nil)
	if err != nil {
		return nil, fmt.Errorf("create human_judge list request: %w", err)
	}

	resp, err := e.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list human_judgments: %w", err)
	}
	defer resp.Body.Close()

	var payload struct {
		Judgments []humanJudgmentRecord `json:"judgments"`
		Error     string                `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode human_judgments: %w", err)
	}
	if resp.StatusCode >= 300 {
		if payload.Error == "" {
			payload.Error = resp.Status
		}
		return nil, fmt.Errorf("list human_judgments: %s", payload.Error)
	}

	for _, judgment := range payload.Judgments {
		if judgment.JudgmentID == judgmentID {
			return &judgment, nil
		}
	}
	return nil, nil
}
