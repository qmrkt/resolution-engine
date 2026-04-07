package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

type BlueprintRunner interface {
	TryStart(appID int) bool
	Finish(appID int)
	RunBlueprint(appID int, blueprintJSON []byte, inputs map[string]string, opts ...RunOptions) (*dag.RunState, error)
	WriteEvidence(appID int, payload interface{}) (string, error)
}

type MarketAutomationService struct {
	runner    BlueprintRunner
	chain     MarketChainReader
	submitter ResolutionSubmitter
	activeMu  sync.Mutex
	activeRan map[int]time.Time
}

func NewMarketAutomationService(
	runner BlueprintRunner,
	chain MarketChainReader,
	submitter ResolutionSubmitter,
) *MarketAutomationService {
	return &MarketAutomationService{
		runner:    runner,
		chain:     chain,
		submitter: submitter,
		activeRan: make(map[int]time.Time),
	}
}

func (s *MarketAutomationService) HandleActive(m MarketInfo) {
	if !s.runner.TryStart(m.AppID) {
		return
	}

	go func() {
		defer s.runner.Finish(m.AppID)
		if err := s.processActive(context.Background(), m); err != nil {
			slog.Error("active market failed", "component", "automation", slog.Int("app_id", m.AppID), "error", err)
		}
	}()
}

func (s *MarketAutomationService) HandlePending(m MarketInfo) {
	if !s.runner.TryStart(m.AppID) {
		return
	}

	go func() {
		defer s.runner.Finish(m.AppID)
		if err := s.processPending(context.Background(), m); err != nil {
			slog.Error("pending market failed", "component", "automation", slog.Int("app_id", m.AppID), "error", err)
		}
	}()
}

func (s *MarketAutomationService) HandleFinalizable(m MarketInfo) {
	if !s.runner.TryStart(m.AppID) {
		return
	}

	go func() {
		defer s.runner.Finish(m.AppID)
		if err := s.processFinalizable(context.Background(), m); err != nil {
			slog.Error("finalize market failed", "component", "automation", slog.Int("app_id", m.AppID), "error", err)
		}
	}()
}

func (s *MarketAutomationService) HandleDisputed(m MarketInfo) {
	if !s.runner.TryStart(m.AppID) {
		return
	}

	go func() {
		defer s.runner.Finish(m.AppID)
		if err := s.processDisputed(context.Background(), m); err != nil {
			slog.Error("dispute market failed", "component", "automation", slog.Int("app_id", m.AppID), "error", err)
		}
	}()
}

func (s *MarketAutomationService) processActive(ctx context.Context, market MarketInfo) error {
	state, err := s.chain.ReadMarketState(ctx, market.AppID)
	if err != nil {
		return err
	}

	nowTS, err := s.chain.CurrentTimestamp(ctx)
	if err != nil {
		return err
	}
	if state.Status != StatusActive {
		return nil
	}
	if state.Deadline > 0 && nowTS >= state.Deadline {
		return nil
	}

	blueprintJSON, err := s.chain.ReadMainBlueprint(ctx, market.AppID)
	if err != nil {
		return err
	}

	activeCfg, err := readActiveMonitoringConfig(blueprintJSON)
	if err != nil {
		return err
	}
	if !activeCfg.Enabled {
		return nil
	}
	if !s.shouldRunActiveCheck(market.AppID, activeCfg.PollInterval) {
		return nil
	}

	run, runErr := s.runner.RunBlueprint(market.AppID, blueprintJSON, buildMarketInputs(market, int64(nowTS)), RunOptions{
		Trace: &TraceMetadata{
			AppID:         market.AppID,
			BlueprintPath: PathMain,
			Initiator:     "automation:active",
		},
	})
	if runErr != nil {
		s.persistEvidence(market.AppID, run)
		return fmt.Errorf("run active blueprint: %w", runErr)
	}
	if !isResolutionSuccessful(run) {
		return nil
	}

	outcome, evidenceHash, err := extractResolutionOutcome(run)
	if err != nil {
		s.persistEvidence(market.AppID, run)
		return err
	}
	s.persistEvidence(market.AppID, run)

	txID, err := s.submitter.ProposeEarlyResolution(ctx, state, outcome, evidenceHash)
	if err != nil {
		return err
	}

	slog.Info("proposed early resolution",
		"component", "automation",
		slog.Int("app_id", market.AppID),
		slog.Int("outcome", outcome),
		slog.String("txid", txID),
	)
	return nil
}

func (s *MarketAutomationService) processPending(ctx context.Context, market MarketInfo) error {
	state, err := s.chain.ReadMarketState(ctx, market.AppID)
	if err != nil {
		return err
	}
	if state.Status != StatusResolutionPending {
		return nil
	}

	nowTS, err := s.chain.CurrentTimestamp(ctx)
	if err != nil {
		return err
	}

	blueprintJSON, err := s.chain.ReadMainBlueprint(ctx, market.AppID)
	if err != nil {
		return err
	}

	run, runErr := s.runner.RunBlueprint(market.AppID, blueprintJSON, buildMarketInputs(market, int64(nowTS)), RunOptions{
		Trace: &TraceMetadata{
			AppID:         market.AppID,
			BlueprintPath: PathMain,
			Initiator:     "automation:pending",
		},
	})
	s.persistEvidence(market.AppID, run)
	if runErr != nil {
		return fmt.Errorf("run main blueprint: %w", runErr)
	}
	if !isResolutionSuccessful(run) {
		if deferred, ok := findRunAction(run, "deferred"); ok {
			slog.Info("main blueprint deferred resolution",
				"component", "automation",
				slog.Int("app_id", market.AppID),
				slog.String("reason", deferred.Reason),
			)
			return nil
		}
		slog.Info("main blueprint produced no proposal", "component", "automation", slog.Int("app_id", market.AppID))
		return nil
	}

	outcome, evidenceHash, err := extractResolutionOutcome(run)
	if err != nil {
		return err
	}

	txID, err := s.submitter.ProposeResolution(ctx, state, outcome, evidenceHash)
	if err != nil {
		return err
	}

	slog.Info("proposed resolution",
		"component", "automation",
		slog.Int("app_id", market.AppID),
		slog.Int("outcome", outcome),
		slog.String("txid", txID),
	)
	return nil
}

func (s *MarketAutomationService) processFinalizable(ctx context.Context, market MarketInfo) error {
	state, err := s.chain.ReadMarketState(ctx, market.AppID)
	if err != nil {
		return err
	}
	if state.Status != StatusResolutionProposed {
		return nil
	}
	if strings.TrimSpace(state.Challenger) != "" {
		return nil
	}

	nowTS, err := s.chain.CurrentTimestamp(ctx)
	if err != nil {
		return err
	}

	readyAt := int64(state.ProposalTimestamp + state.ChallengeWindowSecs)
	if state.ProposalTimestamp == 0 || int64(nowTS) < readyAt {
		return nil
	}

	txID, err := s.submitter.FinalizeResolution(ctx, state)
	if err != nil {
		return err
	}

	slog.Info("finalized resolution",
		"component", "automation",
		slog.Int("app_id", market.AppID),
		slog.String("txid", txID),
	)
	return nil
}

func (s *MarketAutomationService) processDisputed(ctx context.Context, market MarketInfo) error {
	state, err := s.chain.ReadMarketState(ctx, market.AppID)
	if err != nil {
		return err
	}
	if state.Status != StatusDisputed {
		return nil
	}

	nowTS, err := s.chain.CurrentTimestamp(ctx)
	if err != nil {
		return err
	}

	blueprintJSON, err := s.chain.ReadDisputeBlueprint(ctx, market.AppID)
	if err != nil {
		return err
	}

	run, runErr := s.runner.RunBlueprint(market.AppID, blueprintJSON, buildMarketInputs(market, int64(nowTS)), RunOptions{
		Trace: &TraceMetadata{
			AppID:         market.AppID,
			BlueprintPath: PathDispute,
			Initiator:     "automation:disputed",
		},
	})
	s.persistEvidence(market.AppID, run)
	if runErr != nil {
		return fmt.Errorf("run dispute blueprint: %w", runErr)
	}

	if isResolutionSuccessful(run) {
		outcome, rulingHash, err := extractResolutionOutcome(run)
		if err != nil {
			return err
		}
		txID, err := s.submitter.FinalizeDispute(ctx, state, outcome, rulingHash)
		if err != nil {
			return err
		}
		slog.Info("finalized dispute",
			"component", "automation",
			slog.Int("app_id", market.AppID),
			slog.Int("outcome", outcome),
			slog.String("txid", txID),
		)
		return nil
	}

	if _, ok := findRunAction(run, "deferred"); ok {
		if !isEarlyProposal(state) {
			slog.Warn("dispute blueprint deferred a non-early proposal",
				"component", "automation",
				slog.Int("app_id", market.AppID),
			)
			return nil
		}

		rulingHash, err := encodeRunRulingHash(run)
		if err != nil {
			return err
		}
		txID, err := s.submitter.AbortEarlyResolution(ctx, state, rulingHash)
		if err != nil {
			return err
		}
		slog.Info("aborted early resolution after dispute",
			"component", "automation",
			slog.Int("app_id", market.AppID),
			slog.String("txid", txID),
		)
		return nil
	}

	if _, ok := findRunAction(run, "cancelled"); ok {
		rulingHash, err := encodeRunRulingHash(run)
		if err != nil {
			return err
		}
		txID, err := s.submitter.CancelDisputeAndMarket(ctx, state, rulingHash)
		if err != nil {
			return err
		}
		slog.Info("cancelled disputed market",
			"component", "automation",
			slog.Int("app_id", market.AppID),
			slog.String("txid", txID),
		)
		return nil
	}

	slog.Warn("dispute blueprint produced no final action", "component", "automation", slog.Int("app_id", market.AppID))
	return nil
}

func (s *MarketAutomationService) persistEvidence(appID int, run *dag.RunState) {
	if run == nil {
		return
	}
	if _, err := s.runner.WriteEvidence(appID, run); err != nil {
		slog.Error("failed to persist evidence", "component", "automation", slog.Int("app_id", appID), "error", err)
	}
}

type activeMonitoringConfig struct {
	Enabled      bool
	PollInterval time.Duration
}

type blueprintExecutionEnvelope struct {
	Execution *struct {
		ActiveMonitoring *struct {
			Enabled             bool `json:"enabled"`
			PollIntervalSeconds int  `json:"poll_interval_seconds,omitempty"`
		} `json:"active_monitoring,omitempty"`
	} `json:"execution,omitempty"`
}

func readActiveMonitoringConfig(blueprintJSON []byte) (activeMonitoringConfig, error) {
	cfg := activeMonitoringConfig{
		PollInterval: 5 * time.Minute,
	}
	if len(blueprintJSON) == 0 {
		return cfg, nil
	}

	var envelope blueprintExecutionEnvelope
	if err := json.Unmarshal(blueprintJSON, &envelope); err != nil {
		return cfg, fmt.Errorf("parse blueprint execution config: %w", err)
	}
	if envelope.Execution == nil || envelope.Execution.ActiveMonitoring == nil {
		return cfg, nil
	}

	cfg.Enabled = envelope.Execution.ActiveMonitoring.Enabled

	intervalSeconds := envelope.Execution.ActiveMonitoring.PollIntervalSeconds
	if intervalSeconds <= 0 {
		intervalSeconds = 300
	}
	if intervalSeconds < 60 {
		intervalSeconds = 60
	}
	if intervalSeconds > 86400 {
		intervalSeconds = 86400
	}
	cfg.PollInterval = time.Duration(intervalSeconds) * time.Second
	return cfg, nil
}

func (s *MarketAutomationService) shouldRunActiveCheck(appID int, interval time.Duration) bool {
	if interval <= 0 {
		interval = 5 * time.Minute
	}

	now := time.Now()
	s.activeMu.Lock()
	defer s.activeMu.Unlock()

	lastRun, ok := s.activeRan[appID]
	if ok && now.Sub(lastRun) < interval {
		return false
	}

	s.activeRan[appID] = now
	return true
}

func extractResolutionOutcome(run *dag.RunState) (int, []byte, error) {
	if run == nil {
		return 0, nil, fmt.Errorf("run is required")
	}

	submission, ok := findSubmittedResolution(run)
	if !ok {
		return 0, nil, fmt.Errorf("submitted resolution is required")
	}

	rawOutcome := strings.TrimSpace(submission.Outcome)
	if rawOutcome == "" {
		return 0, nil, fmt.Errorf("submit.outcome is required")
	}

	outcome, err := strconv.Atoi(rawOutcome)
	if err != nil {
		return 0, nil, fmt.Errorf("parse submit.outcome %q: %w", rawOutcome, err)
	}

	rawHash := strings.TrimSpace(submission.EvidenceHash)
	if rawHash == "" {
		return 0, nil, fmt.Errorf("submit.evidence_hash is required")
	}

	evidenceHash, err := decodeHexHash(rawHash)
	if err != nil {
		return 0, nil, err
	}
	return outcome, evidenceHash, nil
}

func buildMarketInputs(market MarketInfo, nowTS int64) map[string]string {
	outcomes := parseMarketOutcomes(market.Outcomes)
	indexed := make([]string, 0, len(outcomes))
	for index, outcome := range outcomes {
		indexed = append(indexed, fmt.Sprintf("%d: %s", index, outcome))
	}

	outcomesJSON, _ := json.Marshal(outcomes)
	if nowTS <= 0 {
		nowTS = time.Now().Unix()
	}
	deadlinePassed := market.Deadline > 0 && nowTS >= int64(market.Deadline)
	return map[string]string{
		"market.question":                 market.Question,
		"market.outcomes.csv":             strings.Join(outcomes, ", "),
		"market.outcomes.indexed":         strings.Join(indexed, ", "),
		"market.outcomes.json":            string(outcomesJSON),
		"market.deadline":                 strconv.Itoa(market.Deadline),
		"market.resolution_pending_since": strconv.FormatInt(market.ResolutionPendingSince, 10),
		"market.now_ts":                   strconv.FormatInt(nowTS, 10),
		"market.deadline_passed":          strconv.FormatBool(deadlinePassed),
		"market.status":                   strconv.Itoa(market.Status),
		"market.app_id":                   strconv.Itoa(market.AppID),
		"market_question":                 market.Question,
		"market_outcomes_json":            string(outcomesJSON),
	}
}

func isEarlyProposal(state *MarketChainState) bool {
	if state == nil {
		return false
	}
	if state.ProposalTimestamp == 0 || state.Deadline == 0 {
		return false
	}
	return state.ProposalTimestamp < state.Deadline
}

func parseMarketOutcomes(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	var decoded []string
	if err := json.Unmarshal([]byte(raw), &decoded); err == nil {
		return decoded
	}
	return nil
}
