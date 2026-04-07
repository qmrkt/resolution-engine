package main

import (
	"context"
	"encoding/hex"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/question-market/resolution-engine/dag"
)

type fakeRunner struct {
	run            *dag.RunState
	err            error
	lastInputs     map[string]string
	lastBlueprint  []byte
	lastOpts       RunOptions
	lastAppID      int
	runCount       int
	writeEvidenceN int
}

func (f *fakeRunner) TryStart(appID int) bool { return true }
func (f *fakeRunner) Finish(appID int)        {}

func (f *fakeRunner) RunBlueprint(appID int, blueprintJSON []byte, inputs map[string]string, opts ...RunOptions) (*dag.RunState, error) {
	f.lastAppID = appID
	f.runCount++
	f.lastBlueprint = append([]byte(nil), blueprintJSON...)
	f.lastInputs = make(map[string]string, len(inputs))
	for key, value := range inputs {
		f.lastInputs[key] = value
	}
	f.lastOpts = firstRunOptions(opts...)
	return f.run, f.err
}

func (f *fakeRunner) WriteEvidence(appID int, payload interface{}) (string, error) {
	f.writeEvidenceN++
	return "evidence.json", nil
}

type fakeChain struct {
	state            *MarketChainState
	mainBlueprint    []byte
	disputeBlueprint []byte
	currentTimestamp uint64
}

func (f *fakeChain) ReadMarketState(ctx context.Context, appID int) (*MarketChainState, error) {
	return f.state, nil
}

func (f *fakeChain) ReadMainBlueprint(ctx context.Context, appID int) ([]byte, error) {
	return append([]byte(nil), f.mainBlueprint...), nil
}

func (f *fakeChain) ReadDisputeBlueprint(ctx context.Context, appID int) ([]byte, error) {
	return append([]byte(nil), f.disputeBlueprint...), nil
}

func (f *fakeChain) CurrentTimestamp(ctx context.Context) (uint64, error) {
	if f.currentTimestamp != 0 {
		return f.currentTimestamp, nil
	}
	return uint64(time.Now().Unix()), nil
}

type fakeSubmitter struct {
	address               string
	proposeEarlyCalled    bool
	proposeCalled         bool
	abortEarlyCalled      bool
	finalizeCalled        bool
	finalizeDisputeCalled bool
	cancelDisputeCalled   bool
	lastState             *MarketChainState
	lastOutcome           int
	lastHash              []byte
}

func (f *fakeSubmitter) Address() string { return f.address }

func (f *fakeSubmitter) ProposeEarlyResolution(ctx context.Context, state *MarketChainState, outcome int, evidenceHash []byte) (string, error) {
	f.proposeEarlyCalled = true
	f.lastState = state
	f.lastOutcome = outcome
	f.lastHash = append([]byte(nil), evidenceHash...)
	return "TX-PROPOSE-EARLY", nil
}

func (f *fakeSubmitter) ProposeResolution(ctx context.Context, state *MarketChainState, outcome int, evidenceHash []byte) (string, error) {
	f.proposeCalled = true
	f.lastState = state
	f.lastOutcome = outcome
	f.lastHash = append([]byte(nil), evidenceHash...)
	return "TX-PROPOSE", nil
}

func (f *fakeSubmitter) AbortEarlyResolution(ctx context.Context, state *MarketChainState, rulingHash []byte) (string, error) {
	f.abortEarlyCalled = true
	f.lastState = state
	f.lastHash = append([]byte(nil), rulingHash...)
	return "TX-ABORT", nil
}

func (f *fakeSubmitter) FinalizeResolution(ctx context.Context, state *MarketChainState) (string, error) {
	f.finalizeCalled = true
	f.lastState = state
	return "TX-FINALIZE", nil
}

func (f *fakeSubmitter) FinalizeDispute(ctx context.Context, state *MarketChainState, outcome int, rulingHash []byte) (string, error) {
	f.finalizeDisputeCalled = true
	f.lastState = state
	f.lastOutcome = outcome
	f.lastHash = append([]byte(nil), rulingHash...)
	return "TX-DISPUTE", nil
}

func (f *fakeSubmitter) CancelDisputeAndMarket(ctx context.Context, state *MarketChainState, rulingHash []byte) (string, error) {
	f.cancelDisputeCalled = true
	f.lastState = state
	f.lastHash = append([]byte(nil), rulingHash...)
	return "TX-CANCEL", nil
}

func TestProcessActiveProposesEarlyResolution(t *testing.T) {
	const evidenceHex = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	service := NewMarketAutomationService(
		&fakeRunner{
			run: &dag.RunState{
				Status: "completed",
				Context: map[string]string{
					"submit.outcome":       "0",
					"submit.evidence_hash": evidenceHex,
					"submit.submitted":     "true",
				},
			},
		},
		&fakeChain{
			state: &MarketChainState{
				AppID:               71,
				Status:              StatusActive,
				Deadline:            uint64(time.Now().Add(15 * time.Minute).Unix()),
				NumOutcomes:         2,
				CurrencyASA:         31566704,
				ProposalBond:        1_000_000,
				ResolutionAuthority: "AUTHORITY",
			},
			mainBlueprint: []byte(`{"id":"active","execution":{"active_monitoring":{"enabled":true,"poll_interval_seconds":900}}}`),
		},
		&fakeSubmitter{address: "AUTHORITY"},
	)

	runner := service.runner.(*fakeRunner)
	submitter := service.submitter.(*fakeSubmitter)
	market := MarketInfo{
		AppID:    71,
		Status:   StatusActive,
		Deadline: int(time.Now().Add(15 * time.Minute).Unix()),
		Question: "Will GPT-5 be released before deadline?",
		Outcomes: `["Yes","No"]`,
	}

	if err := service.processActive(context.Background(), market); err != nil {
		t.Fatal(err)
	}

	if !submitter.proposeEarlyCalled {
		t.Fatal("expected ProposeEarlyResolution to be called")
	}
	if submitter.lastOutcome != 0 {
		t.Fatalf("expected outcome 0, got %d", submitter.lastOutcome)
	}
	if got := hex.EncodeToString(submitter.lastHash); got != evidenceHex {
		t.Fatalf("expected evidence hash %s, got %s", evidenceHex, got)
	}
	if runner.lastInputs["market.deadline"] == "" || runner.lastInputs["market.now_ts"] == "" {
		t.Fatal("expected active inputs to include deadline timing context")
	}
	if runner.lastInputs["market.deadline_passed"] != "false" {
		t.Fatalf("expected market.deadline_passed=false, got %q", runner.lastInputs["market.deadline_passed"])
	}
	if runner.writeEvidenceN != 1 {
		t.Fatalf("expected one evidence write on successful early proposal, got %d", runner.writeEvidenceN)
	}
	if runner.lastOpts.Trace == nil || runner.lastOpts.Trace.BlueprintPath != PathMain || runner.lastOpts.Trace.Initiator != "automation:active" {
		t.Fatalf("unexpected active trace metadata: %+v", runner.lastOpts.Trace)
	}
}

func TestProcessActiveNoopDoesNotPersistEvidence(t *testing.T) {
	service := NewMarketAutomationService(
		&fakeRunner{
			run: &dag.RunState{
				Status:  "completed",
				Context: map[string]string{"term.unique_winner": "false"},
			},
		},
		&fakeChain{
			state: &MarketChainState{
				AppID:               72,
				Status:              StatusActive,
				Deadline:            uint64(time.Now().Add(10 * time.Minute).Unix()),
				NumOutcomes:         2,
				ResolutionAuthority: "AUTHORITY",
			},
			mainBlueprint: []byte(`{"id":"active","execution":{"active_monitoring":{"enabled":true}}}`),
		},
		&fakeSubmitter{address: "AUTHORITY"},
	)

	runner := service.runner.(*fakeRunner)
	submitter := service.submitter.(*fakeSubmitter)

	if err := service.processActive(context.Background(), MarketInfo{
		AppID:    72,
		Status:   StatusActive,
		Deadline: int(time.Now().Add(10 * time.Minute).Unix()),
		Question: "Still unresolved?",
		Outcomes: `["Yes","No"]`,
	}); err != nil {
		t.Fatal(err)
	}

	if submitter.proposeEarlyCalled {
		t.Fatal("did not expect ProposeEarlyResolution to be called")
	}
	if runner.writeEvidenceN != 0 {
		t.Fatalf("expected no-op active check to skip evidence persistence, got %d writes", runner.writeEvidenceN)
	}
}

func TestProcessActiveThrottleSkipsFrequentChecks(t *testing.T) {
	service := NewMarketAutomationService(
		&fakeRunner{
			run: &dag.RunState{
				Status: "completed",
				Context: map[string]string{
					"submit.outcome":       "1",
					"submit.evidence_hash": strings.Repeat("b", 64),
					"submit.submitted":     "true",
				},
			},
		},
		&fakeChain{
			state: &MarketChainState{
				AppID:               73,
				Status:              StatusActive,
				Deadline:            uint64(time.Now().Add(10 * time.Minute).Unix()),
				NumOutcomes:         2,
				CurrencyASA:         31566704,
				ProposalBond:        1_000_000,
				ResolutionAuthority: "AUTHORITY",
			},
			mainBlueprint: []byte(`{"id":"active","execution":{"active_monitoring":{"enabled":true,"poll_interval_seconds":3600}}}`),
		},
		&fakeSubmitter{address: "AUTHORITY"},
	)

	runner := service.runner.(*fakeRunner)
	service.activeRan[73] = time.Now()

	if err := service.processActive(context.Background(), MarketInfo{
		AppID:    73,
		Status:   StatusActive,
		Deadline: int(time.Now().Add(10 * time.Minute).Unix()),
		Question: "Should throttle?",
		Outcomes: `["Yes","No"]`,
	}); err != nil {
		t.Fatal(err)
	}

	if runner.runCount != 0 {
		t.Fatalf("expected throttled active check to skip blueprint run, got %d runs", runner.runCount)
	}
}

func TestProcessPendingProposesResolution(t *testing.T) {
	const evidenceHex = "1111111111111111111111111111111111111111111111111111111111111111"

	service := NewMarketAutomationService(
		&fakeRunner{
			run: &dag.RunState{
				Status: "completed",
				Context: map[string]string{
					"submit.outcome":       "1",
					"submit.evidence_hash": evidenceHex,
				},
			},
		},
		&fakeChain{
			state: &MarketChainState{
				AppID:               77,
				Status:              StatusResolutionPending,
				NumOutcomes:         2,
				CurrencyASA:         31566704,
				ProposalBond:        1_000_000,
				ResolutionAuthority: "AUTHORITY",
			},
			mainBlueprint: []byte(`{"id":"main"}`),
		},
		&fakeSubmitter{address: "AUTHORITY"},
	)

	runner := service.runner.(*fakeRunner)
	submitter := service.submitter.(*fakeSubmitter)

	err := service.processPending(context.Background(), MarketInfo{
		AppID:                  77,
		Question:               "Will BTC close above $100k?",
		Outcomes:               `["Yes","No"]`,
		ResolutionPendingSince: 1_712_340_000,
	})
	if err != nil {
		t.Fatal(err)
	}

	if !submitter.proposeCalled {
		t.Fatal("expected ProposeResolution to be called")
	}
	if submitter.lastOutcome != 1 {
		t.Fatalf("expected outcome 1, got %d", submitter.lastOutcome)
	}
	if got := hex.EncodeToString(submitter.lastHash); got != evidenceHex {
		t.Fatalf("expected evidence hash %s, got %s", evidenceHex, got)
	}
	if runner.lastInputs["market.question"] != "Will BTC close above $100k?" {
		t.Fatalf("expected market.question input, got %q", runner.lastInputs["market.question"])
	}
	if runner.lastInputs["market.outcomes.indexed"] != "0: Yes, 1: No" {
		t.Fatalf("expected indexed outcomes, got %q", runner.lastInputs["market.outcomes.indexed"])
	}
	if runner.lastInputs["market.resolution_pending_since"] != "1712340000" {
		t.Fatalf("expected resolution_pending_since input, got %q", runner.lastInputs["market.resolution_pending_since"])
	}
	if runner.writeEvidenceN != 1 {
		t.Fatalf("expected evidence to be persisted once, got %d", runner.writeEvidenceN)
	}
	if runner.lastOpts.Trace == nil || runner.lastOpts.Trace.BlueprintPath != PathMain || runner.lastOpts.Trace.Initiator != "automation:pending" {
		t.Fatalf("unexpected pending trace metadata: %+v", runner.lastOpts.Trace)
	}
}

func TestProcessPendingDeferredDoesNotPropose(t *testing.T) {
	service := NewMarketAutomationService(
		&fakeRunner{
			run: &dag.RunState{
				Status: "completed",
				Context: map[string]string{
					"wait.status":    "waiting",
					"defer.reason":   "evidence window still open",
					"defer.deferred": "true",
				},
			},
		},
		&fakeChain{
			state: &MarketChainState{
				AppID:               78,
				Status:              StatusResolutionPending,
				NumOutcomes:         2,
				CurrencyASA:         31566704,
				ProposalBond:        1_000_000,
				ResolutionAuthority: "AUTHORITY",
			},
			mainBlueprint: []byte(`{"id":"main"}`),
		},
		&fakeSubmitter{address: "AUTHORITY"},
	)

	runner := service.runner.(*fakeRunner)
	submitter := service.submitter.(*fakeSubmitter)

	err := service.processPending(context.Background(), MarketInfo{
		AppID:                  78,
		Question:               "Wait for evidence?",
		Outcomes:               `["Yes","No"]`,
		ResolutionPendingSince: 1_712_340_000,
	})
	if err != nil {
		t.Fatal(err)
	}

	if submitter.proposeCalled {
		t.Fatal("expected deferred run not to propose a resolution")
	}
	if runner.writeEvidenceN != 1 {
		t.Fatalf("expected deferred run evidence to be persisted once, got %d", runner.writeEvidenceN)
	}
}

func TestProcessFinalizableFinalizesResolution(t *testing.T) {
	service := NewMarketAutomationService(
		&fakeRunner{},
		&fakeChain{
			state: &MarketChainState{
				AppID:               81,
				Status:              StatusResolutionProposed,
				NumOutcomes:         2,
				CurrencyASA:         31566704,
				ProposalTimestamp:   uint64(time.Now().Add(-2 * time.Hour).Unix()),
				ChallengeWindowSecs: 300,
				ResolutionAuthority: "AUTHORITY",
			},
		},
		&fakeSubmitter{address: "AUTHORITY"},
	)

	submitter := service.submitter.(*fakeSubmitter)
	if err := service.processFinalizable(context.Background(), MarketInfo{AppID: 81}); err != nil {
		t.Fatal(err)
	}
	if !submitter.finalizeCalled {
		t.Fatal("expected FinalizeResolution to be called")
	}
}

func TestProcessFinalizableSkipsWhenChainStateShowsChallenge(t *testing.T) {
	service := NewMarketAutomationService(
		&fakeRunner{},
		&fakeChain{
			state: &MarketChainState{
				AppID:               82,
				Status:              StatusResolutionProposed,
				NumOutcomes:         2,
				CurrencyASA:         31566704,
				ProposalTimestamp:   uint64(time.Now().Add(-2 * time.Hour).Unix()),
				ChallengeWindowSecs: 300,
				Challenger:          "CHALLENGER",
				ResolutionAuthority: "AUTHORITY",
			},
		},
		&fakeSubmitter{address: "AUTHORITY"},
	)

	submitter := service.submitter.(*fakeSubmitter)
	if err := service.processFinalizable(context.Background(), MarketInfo{AppID: 82}); err != nil {
		t.Fatal(err)
	}
	if submitter.finalizeCalled {
		t.Fatal("expected finalization to be skipped when chain state shows challenger")
	}
}

func TestProcessFinalizableUsesChainTimestampInsteadOfWallClock(t *testing.T) {
	proposalTimestamp := uint64(time.Now().Add(30 * time.Minute).Unix())
	service := NewMarketAutomationService(
		&fakeRunner{},
		&fakeChain{
			state: &MarketChainState{
				AppID:               84,
				Status:              StatusResolutionProposed,
				NumOutcomes:         2,
				CurrencyASA:         31566704,
				ProposalTimestamp:   proposalTimestamp,
				ChallengeWindowSecs: 300,
				ResolutionAuthority: "AUTHORITY",
			},
			currentTimestamp: proposalTimestamp + 301,
		},
		&fakeSubmitter{address: "AUTHORITY"},
	)

	submitter := service.submitter.(*fakeSubmitter)
	if err := service.processFinalizable(context.Background(), MarketInfo{AppID: 84}); err != nil {
		t.Fatal(err)
	}
	if !submitter.finalizeCalled {
		t.Fatal("expected finalization to use chain timestamp readiness")
	}
}

func TestProcessPendingSkipsWhenChainStateNoLongerPending(t *testing.T) {
	service := NewMarketAutomationService(
		&fakeRunner{
			run: &dag.RunState{
				Status: "completed",
				Context: map[string]string{
					"submit.outcome":       "1",
					"submit.evidence_hash": strings.Repeat("1", 64),
				},
			},
		},
		&fakeChain{
			state: &MarketChainState{
				AppID:               83,
				Status:              StatusResolved,
				NumOutcomes:         2,
				CurrencyASA:         31566704,
				ResolutionAuthority: "AUTHORITY",
			},
			mainBlueprint: []byte(`{"id":"main"}`),
		},
		&fakeSubmitter{address: "AUTHORITY"},
	)

	runner := service.runner.(*fakeRunner)
	submitter := service.submitter.(*fakeSubmitter)
	if err := service.processPending(context.Background(), MarketInfo{AppID: 83}); err != nil {
		t.Fatal(err)
	}
	if submitter.proposeCalled {
		t.Fatal("expected proposal to be skipped when chain state is no longer pending")
	}
	if runner.lastAppID != 0 {
		t.Fatal("expected runner not to execute when chain state is no longer pending")
	}
}

func TestProcessActiveUsesChainTimestampForDeadlineGate(t *testing.T) {
	deadline := uint64(time.Now().Add(30 * time.Minute).Unix())
	service := NewMarketAutomationService(
		&fakeRunner{
			run: &dag.RunState{
				Status: "completed",
				Context: map[string]string{
					"submit.outcome":       "0",
					"submit.evidence_hash": strings.Repeat("a", 64),
					"submit.submitted":     "true",
				},
			},
		},
		&fakeChain{
			state: &MarketChainState{
				AppID:               85,
				Status:              StatusActive,
				Deadline:            deadline,
				NumOutcomes:         2,
				CurrencyASA:         31566704,
				ProposalBond:        1_000_000,
				ResolutionAuthority: "AUTHORITY",
			},
			mainBlueprint:    []byte(`{"id":"active","execution":{"active_monitoring":{"enabled":true}}}`),
			currentTimestamp: deadline + 1,
		},
		&fakeSubmitter{address: "AUTHORITY"},
	)

	runner := service.runner.(*fakeRunner)
	submitter := service.submitter.(*fakeSubmitter)
	err := service.processActive(context.Background(), MarketInfo{
		AppID:    85,
		Status:   StatusActive,
		Deadline: int(deadline),
		Question: "Should chain time gate active monitoring?",
		Outcomes: `["Yes","No"]`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if runner.runCount != 0 {
		t.Fatal("expected active monitoring to skip once chain deadline has passed")
	}
	if submitter.proposeEarlyCalled {
		t.Fatal("did not expect early proposal after chain deadline")
	}
}

func TestProcessDisputedFinalizesAuthorityPath(t *testing.T) {
	const evidenceHex = "2222222222222222222222222222222222222222222222222222222222222222"

	service := NewMarketAutomationService(
		&fakeRunner{
			run: &dag.RunState{
				Status: "completed",
				Context: map[string]string{
					"submit.outcome":       "0",
					"submit.evidence_hash": evidenceHex,
					"judge.responder_role": "creator",
				},
			},
		},
		&fakeChain{
			state: &MarketChainState{
				AppID:               91,
				Status:              StatusDisputed,
				NumOutcomes:         2,
				CurrencyASA:         31566704,
				Proposer:            "PROP",
				Challenger:          "CHAL",
				ResolutionAuthority: "AUTHORITY",
			},
			disputeBlueprint: []byte(`{"id":"dispute"}`),
		},
		&fakeSubmitter{address: "AUTHORITY"},
	)

	submitter := service.submitter.(*fakeSubmitter)
	if err := service.processDisputed(context.Background(), MarketInfo{
		AppID:    91,
		Question: "Did the event happen?",
		Outcomes: `["Yes","No"]`,
	}); err != nil {
		t.Fatal(err)
	}
	if !submitter.finalizeDisputeCalled {
		t.Fatal("expected FinalizeDispute to be called")
	}
	if submitter.cancelDisputeCalled {
		t.Fatal("did not expect CancelDisputeAndMarket to be called")
	}
	if submitter.lastOutcome != 0 {
		t.Fatalf("expected dispute outcome 0, got %d", submitter.lastOutcome)
	}
	if got := hex.EncodeToString(submitter.lastHash); got != evidenceHex {
		t.Fatalf("expected ruling hash %s, got %s", evidenceHex, got)
	}
	runner := service.runner.(*fakeRunner)
	if runner.lastOpts.Trace == nil || runner.lastOpts.Trace.BlueprintPath != PathDispute || runner.lastOpts.Trace.Initiator != "automation:disputed" {
		t.Fatalf("unexpected disputed trace metadata: %+v", runner.lastOpts.Trace)
	}
}

func TestProcessDisputedCancelsWhenBlueprintCancels(t *testing.T) {
	service := NewMarketAutomationService(
		&fakeRunner{
			run: &dag.RunState{
				Status: "completed",
				Context: map[string]string{
					"cancel_market.cancelled": "true",
					"cancel_market.reason":    "no reliable evidence",
				},
			},
		},
		&fakeChain{
			state: &MarketChainState{
				AppID:               99,
				Status:              StatusDisputed,
				NumOutcomes:         2,
				CurrencyASA:         31566704,
				ResolutionAuthority: "AUTHORITY",
			},
			disputeBlueprint: []byte(`{"id":"dispute"}`),
		},
		&fakeSubmitter{address: "AUTHORITY"},
	)

	submitter := service.submitter.(*fakeSubmitter)
	if err := service.processDisputed(context.Background(), MarketInfo{
		AppID:    99,
		Question: "Will this be cancelled?",
		Outcomes: `["Yes","No"]`,
	}); err != nil {
		t.Fatal(err)
	}
	if !submitter.cancelDisputeCalled {
		t.Fatal("expected CancelDisputeAndMarket to be called")
	}
	if submitter.finalizeDisputeCalled {
		t.Fatal("did not expect FinalizeDispute to be called")
	}
	if len(submitter.lastHash) != 32 {
		t.Fatalf("expected 32-byte ruling hash, got %d bytes", len(submitter.lastHash))
	}
}

func TestProcessDisputedDefersEarlyProposal(t *testing.T) {
	service := NewMarketAutomationService(
		&fakeRunner{
			run: &dag.RunState{
				Status: "completed",
				Context: map[string]string{
					"defer.deferred": "true",
					"defer.reason":   "not terminal yet",
				},
			},
		},
		&fakeChain{
			state: &MarketChainState{
				AppID:               100,
				Status:              StatusDisputed,
				Deadline:            uint64(time.Now().Add(30 * time.Minute).Unix()),
				ProposalTimestamp:   uint64(time.Now().Add(-5 * time.Minute).Unix()),
				NumOutcomes:         2,
				CurrencyASA:         31566704,
				Challenger:          "CHAL",
				ResolutionAuthority: "AUTHORITY",
			},
			disputeBlueprint: []byte(`{"id":"dispute"}`),
		},
		&fakeSubmitter{address: "AUTHORITY"},
	)

	submitter := service.submitter.(*fakeSubmitter)
	if err := service.processDisputed(context.Background(), MarketInfo{
		AppID:    100,
		Status:   StatusDisputed,
		Question: "Did the event happen yet?",
		Outcomes: `["Yes","No"]`,
	}); err != nil {
		t.Fatal(err)
	}

	if !submitter.abortEarlyCalled {
		t.Fatal("expected AbortEarlyResolution to be called")
	}
	if submitter.finalizeDisputeCalled || submitter.cancelDisputeCalled {
		t.Fatal("did not expect finalize_dispute or cancel_dispute_and_market")
	}
	if len(submitter.lastHash) != 32 {
		t.Fatalf("expected 32-byte ruling hash, got %d bytes", len(submitter.lastHash))
	}
}

func TestProcessDisputedIgnoresDeferForNonEarlyProposal(t *testing.T) {
	service := NewMarketAutomationService(
		&fakeRunner{
			run: &dag.RunState{
				Status: "completed",
				Context: map[string]string{
					"defer.deferred": "true",
					"defer.reason":   "not terminal yet",
				},
			},
		},
		&fakeChain{
			state: &MarketChainState{
				AppID:               101,
				Status:              StatusDisputed,
				Deadline:            uint64(time.Now().Add(-30 * time.Minute).Unix()),
				ProposalTimestamp:   uint64(time.Now().Add(-5 * time.Minute).Unix()),
				NumOutcomes:         2,
				CurrencyASA:         31566704,
				ResolutionAuthority: "AUTHORITY",
			},
			disputeBlueprint: []byte(`{"id":"dispute"}`),
		},
		&fakeSubmitter{address: "AUTHORITY"},
	)

	submitter := service.submitter.(*fakeSubmitter)
	if err := service.processDisputed(context.Background(), MarketInfo{
		AppID:    101,
		Status:   StatusDisputed,
		Question: "Should defer be ignored?",
		Outcomes: `["Yes","No"]`,
	}); err != nil {
		t.Fatal(err)
	}

	if submitter.abortEarlyCalled || submitter.finalizeDisputeCalled || submitter.cancelDisputeCalled {
		t.Fatal("expected non-early defer to be ignored")
	}
}

func TestBuildMarketInputsParsesOutcomeLabels(t *testing.T) {
	deadline := int(time.Now().Add(2 * time.Hour).Unix())
	inputs := buildMarketInputs(MarketInfo{
		AppID:    12,
		Status:   StatusActive,
		Deadline: deadline,
		Question: "Will the market resolve?",
		Outcomes: `["Yes","No","Maybe"]`,
	}, int64(deadline-60))

	if inputs["market.question"] != "Will the market resolve?" {
		t.Fatalf("unexpected market.question %q", inputs["market.question"])
	}
	if !strings.Contains(inputs["market.outcomes.indexed"], "2: Maybe") {
		t.Fatalf("expected indexed outcomes to include Maybe, got %q", inputs["market.outcomes.indexed"])
	}
	if inputs["market.app_id"] != "12" || inputs["market.status"] != "1" {
		t.Fatalf("expected market identity inputs, got app_id=%q status=%q", inputs["market.app_id"], inputs["market.status"])
	}
	if inputs["market.deadline"] == "" || inputs["market.now_ts"] == "" {
		t.Fatal("expected timing inputs to be populated")
	}
	if inputs["market.deadline_passed"] != "false" {
		t.Fatalf("expected market.deadline_passed=false, got %q", inputs["market.deadline_passed"])
	}
}

func TestBuildMarketInputsUsesProvidedChainTimestamp(t *testing.T) {
	deadline := int(time.Now().Add(10 * time.Minute).Unix())
	inputs := buildMarketInputs(MarketInfo{
		AppID:    13,
		Status:   StatusActive,
		Deadline: deadline,
		Question: "Did chain time win?",
		Outcomes: `["Yes","No"]`,
	}, int64(deadline+5))

	if inputs["market.now_ts"] != strconv.FormatInt(int64(deadline+5), 10) {
		t.Fatalf("expected provided chain timestamp, got %q", inputs["market.now_ts"])
	}
	if inputs["market.deadline_passed"] != "true" {
		t.Fatalf("expected market.deadline_passed=true, got %q", inputs["market.deadline_passed"])
	}
}
