package main

import (
	"bytes"
	"testing"

	algocrypto "github.com/algorand/go-algorand-sdk/v2/crypto"
)

func TestPendingPayoutBoxReferencesUsesLedgerPrefixAndDeduplicates(t *testing.T) {
	account := algocrypto.GenerateAccount()
	refs := pendingPayoutBoxReferences(42, account.Address.String(), account.Address.String(), "")
	if len(refs) != 1 {
		t.Fatalf("expected one deduplicated payout box ref, got %d", len(refs))
	}
	if refs[0].AppID != 42 {
		t.Fatalf("expected app id 42, got %d", refs[0].AppID)
	}
	if !bytes.HasPrefix(refs[0].Name, []byte("pending_payouts:")) {
		t.Fatalf("expected pending payout prefix, got %x", refs[0].Name)
	}
	if len(refs[0].Name) != len("pending_payouts:")+32 {
		t.Fatalf("expected payout box name to contain a 32-byte address, got %d bytes", len(refs[0].Name))
	}
}

func TestPayoutRecipientUsesOriginalProposal(t *testing.T) {
	state := &MarketChainState{
		ProposedOutcome: 1,
		Proposer:        "PROP",
		Challenger:      "CHAL",
	}

	if got := payoutRecipient(state, 1); got != "PROP" {
		t.Fatalf("expected proposer payout for matching outcome, got %q", got)
	}
	if got := payoutRecipient(state, 0); got != "CHAL" {
		t.Fatalf("expected challenger payout for overturned outcome, got %q", got)
	}
}
