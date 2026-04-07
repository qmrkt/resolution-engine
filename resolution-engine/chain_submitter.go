package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	algoabi "github.com/algorand/go-algorand-sdk/v2/abi"
	algod "github.com/algorand/go-algorand-sdk/v2/client/v2/algod"
	algomodels "github.com/algorand/go-algorand-sdk/v2/client/v2/common/models"
	algocrypto "github.com/algorand/go-algorand-sdk/v2/crypto"
	algomnemonic "github.com/algorand/go-algorand-sdk/v2/mnemonic"
	algotxn "github.com/algorand/go-algorand-sdk/v2/transaction"
	algotypes "github.com/algorand/go-algorand-sdk/v2/types"
)

const (
	boxKeyMainBlueprint    = "main_blueprint"
	boxKeyDisputeBlueprint = "dispute_blueprint"

	methodSigNoop                   = "noop()void"
	methodSigProposeEarlyResolution = "propose_early_resolution(uint64,byte[],axfer)void"
	methodSigProposeResolution      = "propose_resolution(uint64,byte[],axfer)void"
	methodSigAbortEarlyResolution   = "abort_early_resolution(byte[])void"
	methodSigFinalizeResolution     = "finalize_resolution()void"
	methodSigFinalizeDispute        = "finalize_dispute(uint64,byte[])void"
	methodSigCancelDisputeAndMarket = "cancel_dispute_and_market(byte[])void"
)

type MarketChainState struct {
	AppID               int
	Status              int
	Deadline            uint64
	NumOutcomes         int
	CurrencyASA         uint64
	ProposalBond        uint64
	ChallengeWindowSecs uint64
	ProposalTimestamp   uint64
	ProposedOutcome     uint64
	Proposer            string
	Challenger          string
	Creator             string
	MarketAdmin         string
	ResolutionAuthority string
}

type MarketChainReader interface {
	ReadMarketState(ctx context.Context, appID int) (*MarketChainState, error)
	ReadMainBlueprint(ctx context.Context, appID int) ([]byte, error)
	ReadDisputeBlueprint(ctx context.Context, appID int) ([]byte, error)
	CurrentTimestamp(ctx context.Context) (uint64, error)
}

type AlgodMarketClient struct {
	algod *algod.Client
}

func NewAlgodMarketClient(server, port, token string) (*AlgodMarketClient, error) {
	address := strings.TrimRight(server, "/")
	if port != "" && port != "80" && port != "443" && !strings.Contains(address, "://") {
		address += ":" + port
	} else if port != "" && port != "80" && port != "443" && !strings.Contains(address[strings.Index(address, "://")+3:], ":") {
		address += ":" + port
	}

	client, err := algod.MakeClient(address, token)
	if err != nil {
		return nil, fmt.Errorf("make algod client: %w", err)
	}
	return &AlgodMarketClient{algod: client}, nil
}

func (c *AlgodMarketClient) ReadMainBlueprint(ctx context.Context, appID int) ([]byte, error) {
	return c.readBox(ctx, appID, []byte(boxKeyMainBlueprint))
}

func (c *AlgodMarketClient) ReadDisputeBlueprint(ctx context.Context, appID int) ([]byte, error) {
	return c.readBox(ctx, appID, []byte(boxKeyDisputeBlueprint))
}

func (c *AlgodMarketClient) ReadMarketState(ctx context.Context, appID int) (*MarketChainState, error) {
	app, err := c.algod.GetApplicationByID(uint64(appID)).Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("get application %d: %w", appID, err)
	}

	state := decodeGlobalState(app.Params.GlobalState)
	numOutcomes := int(getUint64State(state, "num_outcomes"))
	if numOutcomes <= 0 {
		numOutcomes = 2
	}

	return &MarketChainState{
		AppID:               appID,
		Status:              int(getUint64State(state, "status")),
		Deadline:            getUint64State(state, "deadline"),
		NumOutcomes:         numOutcomes,
		CurrencyASA:         getUint64State(state, "currency_asa"),
		ProposalBond:        getUint64State(state, "proposal_bond"),
		ChallengeWindowSecs: getUint64State(state, "challenge_window_secs"),
		ProposalTimestamp:   getUint64StateAny(state, "pts", "proposal_timestamp"),
		ProposedOutcome:     getUint64State(state, "proposed_outcome"),
		Proposer:            getAddressState(state, "proposer"),
		Challenger:          getAddressState(state, "challenger"),
		Creator:             getAddressState(state, "creator"),
		MarketAdmin:         getAddressState(state, "market_admin"),
		ResolutionAuthority: getAddressState(state, "resolution_authority"),
	}, nil
}

func (c *AlgodMarketClient) CurrentTimestamp(ctx context.Context) (uint64, error) {
	status, err := c.algod.Status().Do(ctx)
	if err != nil {
		return 0, fmt.Errorf("algod status: %w", err)
	}

	block, err := c.algod.Block(status.LastRound).Do(ctx)
	if err != nil {
		return 0, fmt.Errorf("read block %d: %w", status.LastRound, err)
	}

	return uint64(block.TimeStamp), nil
}

func (c *AlgodMarketClient) readBox(ctx context.Context, appID int, name []byte) ([]byte, error) {
	box, err := c.algod.GetApplicationBoxByName(uint64(appID), name).Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("read box %q for app %d: %w", string(name), appID, err)
	}
	return box.Value, nil
}

func decodeGlobalState(values []algomodels.TealKeyValue) map[string]interface{} {
	state := make(map[string]interface{}, len(values))
	for _, kv := range values {
		keyBytes, err := base64.StdEncoding.DecodeString(kv.Key)
		if err != nil {
			continue
		}

		key := string(keyBytes)
		if kv.Value.Type == 2 {
			state[key] = kv.Value.Uint
			continue
		}

		raw, err := base64.StdEncoding.DecodeString(kv.Value.Bytes)
		if err != nil {
			continue
		}
		state[key] = raw
	}
	return state
}

func getUint64State(state map[string]interface{}, key string) uint64 {
	if value, ok := state[key]; ok {
		if raw, ok := value.(uint64); ok {
			return raw
		}
	}
	return 0
}

func getUint64StateAny(state map[string]interface{}, keys ...string) uint64 {
	for _, key := range keys {
		if value, ok := state[key]; ok {
			if raw, ok := value.(uint64); ok {
				return raw
			}
		}
	}
	return 0
}

func getBytesState(state map[string]interface{}, key string) []byte {
	if value, ok := state[key]; ok {
		if raw, ok := value.([]byte); ok {
			return raw
		}
	}
	return nil
}

func getAddressState(state map[string]interface{}, key string) string {
	raw := getBytesState(state, key)
	if len(raw) != 32 {
		return ""
	}
	address, err := algotypes.EncodeAddress(raw)
	if err != nil {
		return ""
	}
	zero, _ := algotypes.EncodeAddress(make([]byte, 32))
	if address == zero {
		return ""
	}
	return address
}

type ResolutionSubmitter interface {
	ProposeEarlyResolution(ctx context.Context, state *MarketChainState, outcome int, evidenceHash []byte) (string, error)
	ProposeResolution(ctx context.Context, state *MarketChainState, outcome int, evidenceHash []byte) (string, error)
	AbortEarlyResolution(ctx context.Context, state *MarketChainState, rulingHash []byte) (string, error)
	FinalizeResolution(ctx context.Context, state *MarketChainState) (string, error)
	FinalizeDispute(ctx context.Context, state *MarketChainState, outcome int, rulingHash []byte) (string, error)
	CancelDisputeAndMarket(ctx context.Context, state *MarketChainState, rulingHash []byte) (string, error)
	Address() string
}

type AuthoritySubmitter struct {
	algod   *algod.Client
	account algocrypto.Account
	signer  algotxn.BasicAccountTransactionSigner
	methods map[string]algoabi.Method
}

func NewAuthoritySubmitter(client *AlgodMarketClient, privateKeyB64 string, mnemonic string) (*AuthoritySubmitter, error) {
	account, err := loadAuthorityAccount(privateKeyB64, mnemonic)
	if err != nil {
		return nil, err
	}

	methods := make(map[string]algoabi.Method, 7)
	for _, sig := range []string{
		methodSigNoop,
		methodSigProposeEarlyResolution,
		methodSigProposeResolution,
		methodSigAbortEarlyResolution,
		methodSigFinalizeResolution,
		methodSigFinalizeDispute,
		methodSigCancelDisputeAndMarket,
	} {
		method, err := algoabi.MethodFromSignature(sig)
		if err != nil {
			return nil, fmt.Errorf("parse ABI method %q: %w", sig, err)
		}
		methods[method.Name] = method
	}

	return &AuthoritySubmitter{
		algod:   client.algod,
		account: account,
		signer:  algotxn.BasicAccountTransactionSigner{Account: account},
		methods: methods,
	}, nil
}

func (s *AuthoritySubmitter) Address() string {
	return s.account.Address.String()
}

func (s *AuthoritySubmitter) ProposeEarlyResolution(ctx context.Context, state *MarketChainState, outcome int, evidenceHash []byte) (string, error) {
	if err := s.requireAuthority(state); err != nil {
		return "", err
	}

	sp, err := s.algod.SuggestedParams().Do(ctx)
	if err != nil {
		return "", fmt.Errorf("suggested params: %w", err)
	}

	paymentTxn, err := algotxn.MakeAssetTransferTxn(
		s.Address(),
		algocrypto.GetApplicationAddress(uint64(state.AppID)).String(),
		state.ProposalBond,
		nil,
		sp,
		"",
		state.CurrencyASA,
	)
	if err != nil {
		return "", fmt.Errorf("make proposal bond transfer: %w", err)
	}

	return s.executeCall(
		ctx,
		state,
		"propose_early_resolution",
		[]interface{}{
			uint64(outcome),
			append([]byte(nil), evidenceHash...),
			algotxn.TransactionWithSigner{Txn: paymentTxn, Signer: s.signer},
		},
		callOptions{},
	)
}

func (s *AuthoritySubmitter) ProposeResolution(ctx context.Context, state *MarketChainState, outcome int, evidenceHash []byte) (string, error) {
	if err := s.requireAuthority(state); err != nil {
		return "", err
	}

	sp, err := s.algod.SuggestedParams().Do(ctx)
	if err != nil {
		return "", fmt.Errorf("suggested params: %w", err)
	}

	paymentTxn, err := algotxn.MakeAssetTransferTxn(
		s.Address(),
		algocrypto.GetApplicationAddress(uint64(state.AppID)).String(),
		state.ProposalBond,
		nil,
		sp,
		"",
		state.CurrencyASA,
	)
	if err != nil {
		return "", fmt.Errorf("make proposal bond transfer: %w", err)
	}

	return s.executeCall(
		ctx,
		state,
		"propose_resolution",
		[]interface{}{
			uint64(outcome),
			append([]byte(nil), evidenceHash...),
			algotxn.TransactionWithSigner{Txn: paymentTxn, Signer: s.signer},
		},
		callOptions{},
	)
}

func (s *AuthoritySubmitter) AbortEarlyResolution(ctx context.Context, state *MarketChainState, rulingHash []byte) (string, error) {
	if err := s.requireAuthority(state); err != nil {
		return "", err
	}

	return s.executeCall(
		ctx,
		state,
		"abort_early_resolution",
		[]interface{}{append([]byte(nil), rulingHash...)},
		callOptions{
			foreignAssets:   singleUint64(state.CurrencyASA),
			foreignAccounts: methodAccounts(s.Address(), state.Challenger),
			extraBoxes:      pendingPayoutBoxReferences(uint64(state.AppID), state.Challenger),
		},
	)
}

func (s *AuthoritySubmitter) FinalizeResolution(ctx context.Context, state *MarketChainState) (string, error) {
	if err := s.requireAuthority(state); err != nil {
		return "", err
	}

	return s.executeCall(
		ctx,
		state,
		"finalize_resolution",
		nil,
		callOptions{
			foreignAssets:   singleUint64(state.CurrencyASA),
			foreignAccounts: methodAccounts(s.Address(), state.Proposer),
			extraBoxes:      pendingPayoutBoxReferences(uint64(state.AppID), state.Proposer),
		},
	)
}

func (s *AuthoritySubmitter) FinalizeDispute(ctx context.Context, state *MarketChainState, outcome int, rulingHash []byte) (string, error) {
	if err := s.requireAuthority(state); err != nil {
		return "", err
	}

	return s.executeCall(
		ctx,
		state,
		"finalize_dispute",
		[]interface{}{uint64(outcome), append([]byte(nil), rulingHash...)},
		callOptions{
			foreignAssets:   singleUint64(state.CurrencyASA),
			foreignAccounts: methodAccounts(s.Address(), state.Proposer, state.Challenger),
			extraBoxes:      pendingPayoutBoxReferences(uint64(state.AppID), payoutRecipient(state, outcome)),
		},
	)
}

func (s *AuthoritySubmitter) CancelDisputeAndMarket(ctx context.Context, state *MarketChainState, rulingHash []byte) (string, error) {
	if err := s.requireAuthority(state); err != nil {
		return "", err
	}

	return s.executeCall(
		ctx,
		state,
		"cancel_dispute_and_market",
		[]interface{}{append([]byte(nil), rulingHash...)},
		callOptions{
			foreignAssets:   singleUint64(state.CurrencyASA),
			foreignAccounts: methodAccounts(s.Address(), state.Proposer, state.Challenger),
			extraBoxes:      pendingPayoutBoxReferences(uint64(state.AppID), state.Challenger),
		},
	)
}

func (s *AuthoritySubmitter) requireAuthority(state *MarketChainState) error {
	if state == nil {
		return fmt.Errorf("market state is required")
	}
	if state.ResolutionAuthority == "" {
		return nil
	}
	if state.ResolutionAuthority != s.Address() {
		return fmt.Errorf(
			"resolution authority mismatch: engine signer=%s market=%s",
			s.Address(),
			state.ResolutionAuthority,
		)
	}
	return nil
}

type callOptions struct {
	foreignAssets   []uint64
	foreignAccounts []string
	innerTxnCount   int
	extraBoxes      []algotypes.AppBoxReference
}

func (s *AuthoritySubmitter) executeCall(
	ctx context.Context,
	state *MarketChainState,
	methodName string,
	args []interface{},
	opts callOptions,
) (string, error) {
	method, ok := s.methods[methodName]
	if !ok {
		return "", fmt.Errorf("ABI method %q is not loaded", methodName)
	}

	atc := algotxn.AtomicTransactionComposer{}
	sp, err := s.algod.SuggestedParams().Do(ctx)
	if err != nil {
		return "", fmt.Errorf("suggested params: %w", err)
	}

	allBoxes := append(qBoxReferences(uint64(state.AppID), state.NumOutcomes), opts.extraBoxes...)
	const maxRefsPerTxn = 8

	noopCount := noopsFor(state.NumOutcomes)
	effectiveNoops := noopCount
	if needed := (len(allBoxes) + maxRefsPerTxn - 1) / maxRefsPerTxn; needed > effectiveNoops {
		effectiveNoops = needed
	}

	noopMethod := s.methods["noop"]
	for i := 0; i < effectiveNoops; i++ {
		start := i * maxRefsPerTxn
		end := start + maxRefsPerTxn
		if end > len(allBoxes) {
			end = len(allBoxes)
		}

		params := algotxn.AddMethodCallParams{
			AppID:           uint64(state.AppID),
			Method:          noopMethod,
			MethodArgs:      []interface{}{},
			Sender:          s.account.Address,
			SuggestedParams: sp,
			OnComplete:      algotypes.NoOpOC,
			Signer:          s.signer,
			Note:            []byte(fmt.Sprintf("n%s%d", methodName, i)),
		}
		if end > start {
			params.BoxReferences = append([]algotypes.AppBoxReference(nil), allBoxes[start:end]...)
		}
		if err := atc.AddMethodCall(params); err != nil {
			return "", fmt.Errorf("add noop for %s: %w", methodName, err)
		}
	}

	callSP := sp
	if opts.innerTxnCount > 0 {
		callSP.FlatFee = true
		callSP.Fee = algotypes.MicroAlgos(uint64(1+opts.innerTxnCount) * 1000)
	}

	methodBoxes := allBoxes
	limit := maxRefsPerTxn - len(opts.foreignAssets) - len(opts.foreignAccounts)
	if limit < 0 {
		limit = 0
	}
	if len(methodBoxes) > limit {
		methodBoxes = methodBoxes[:limit]
	}

	params := algotxn.AddMethodCallParams{
		AppID:           uint64(state.AppID),
		Method:          method,
		MethodArgs:      args,
		Sender:          s.account.Address,
		SuggestedParams: callSP,
		OnComplete:      algotypes.NoOpOC,
		Signer:          s.signer,
	}
	if len(methodBoxes) > 0 {
		params.BoxReferences = append([]algotypes.AppBoxReference(nil), methodBoxes...)
	}
	if len(opts.foreignAssets) > 0 {
		params.ForeignAssets = append([]uint64(nil), opts.foreignAssets...)
	}
	if len(opts.foreignAccounts) > 0 {
		params.ForeignAccounts = append([]string(nil), opts.foreignAccounts...)
	}

	if err := atc.AddMethodCall(params); err != nil {
		return "", fmt.Errorf("add method call %s: %w", methodName, err)
	}

	result, err := atc.Execute(s.algod, ctx, 4)
	if err != nil {
		return "", fmt.Errorf("execute %s: %w", methodName, err)
	}
	return result.TxIDs[len(result.TxIDs)-1], nil
}

func loadAuthorityAccount(privateKeyB64 string, mnemonicValue string) (algocrypto.Account, error) {
	if strings.TrimSpace(privateKeyB64) != "" {
		raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(privateKeyB64))
		if err != nil {
			return algocrypto.Account{}, fmt.Errorf("decode authority private key: %w", err)
		}
		account, err := algocrypto.AccountFromPrivateKey(ed25519.PrivateKey(raw))
		if err != nil {
			return algocrypto.Account{}, fmt.Errorf("load authority private key: %w", err)
		}
		return account, nil
	}

	if strings.TrimSpace(mnemonicValue) != "" {
		privateKey, err := algomnemonic.ToPrivateKey(strings.TrimSpace(mnemonicValue))
		if err != nil {
			return algocrypto.Account{}, fmt.Errorf("decode authority mnemonic: %w", err)
		}
		account, err := algocrypto.AccountFromPrivateKey(privateKey)
		if err != nil {
			return algocrypto.Account{}, fmt.Errorf("load authority account from mnemonic: %w", err)
		}
		return account, nil
	}

	return algocrypto.Account{}, fmt.Errorf(
		"resolution authority signer is not configured; set RESOLUTION_AUTHORITY_PRIVATE_KEY, RESOLUTION_AUTHORITY_PRIVATE_KEY_B64, RESOLUTION_AUTHORITY_MNEMONIC, or AVM_PRIVATE_KEY",
	)
}

func qBoxReferences(appID uint64, numOutcomes int) []algotypes.AppBoxReference {
	if numOutcomes <= 0 {
		numOutcomes = 2
	}
	refs := make([]algotypes.AppBoxReference, 0, numOutcomes)
	for i := 0; i < numOutcomes; i++ {
		refs = append(refs, algotypes.AppBoxReference{
			AppID: appID,
			Name:  boxNameForUint64("q", uint64(i)),
		})
	}
	return refs
}

func boxNameForUint64(prefix string, value uint64) []byte {
	name := make([]byte, len(prefix)+8)
	copy(name, []byte(prefix))
	binary.BigEndian.PutUint64(name[len(prefix):], value)
	return name
}

func boxNameForAddress(prefix string, address string) []byte {
	address = strings.TrimSpace(address)
	if address == "" {
		return nil
	}

	decoded, err := algotypes.DecodeAddress(address)
	if err != nil {
		return nil
	}

	name := make([]byte, len(prefix)+len(decoded))
	copy(name, []byte(prefix))
	copy(name[len(prefix):], decoded[:])
	return name
}

func pendingPayoutBoxReferences(appID uint64, accounts ...string) []algotypes.AppBoxReference {
	if appID == 0 || len(accounts) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(accounts))
	refs := make([]algotypes.AppBoxReference, 0, len(accounts))
	for _, account := range accounts {
		name := boxNameForAddress("pending_payouts:", account)
		if len(name) == 0 {
			continue
		}
		key := hex.EncodeToString(name)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		refs = append(refs, algotypes.AppBoxReference{
			AppID: appID,
			Name:  name,
		})
	}
	return refs
}

func methodAccounts(sender string, accounts ...string) []string {
	seen := map[string]struct{}{strings.TrimSpace(sender): {}}
	result := make([]string, 0, len(accounts))
	for _, account := range accounts {
		account = strings.TrimSpace(account)
		if account == "" {
			continue
		}
		if _, ok := seen[account]; ok {
			continue
		}
		seen[account] = struct{}{}
		result = append(result, account)
	}
	return result
}

func payoutRecipient(state *MarketChainState, resolvedOutcome int) string {
	if state == nil {
		return ""
	}
	if uint64(resolvedOutcome) == state.ProposedOutcome {
		return state.Proposer
	}
	return state.Challenger
}

func noopsFor(numOutcomes int) int {
	if numOutcomes <= 2 {
		return 10
	}
	if numOutcomes <= 3 {
		return 14
	}
	return 14
}

func singleUint64(value uint64) []uint64 {
	if value == 0 {
		return nil
	}
	return []uint64{value}
}

func decodeHexHash(value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, fmt.Errorf("hash is required")
	}
	raw, err := hex.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("decode hash %q: %w", value, err)
	}
	return raw, nil
}

func encodeRunRulingHash(run interface{}) ([]byte, error) {
	payload, err := json.Marshal(run)
	if err != nil {
		return nil, fmt.Errorf("marshal ruling payload: %w", err)
	}
	sum := EvidenceHashBytes(payload)
	return sum[:], nil
}

func EvidenceHashBytes(payload []byte) [32]byte {
	return sha256.Sum256(payload)
}
