package main

import (
	"strings"
	"testing"

	"github.com/qmrkt/resolution-engine/dag"
	"pgregory.net/rapid"
)

// waitingNodeGen produces durableWaitingNode values spanning all three Kind
// cases ("signal", "timer", ""), with empty/absent optional fields left as nil
// so invariants hold without nil-vs-empty pedantry.
func waitingNodeGen() *rapid.Generator[durableWaitingNode] {
	return rapid.Custom[durableWaitingNode](func(t *rapid.T) durableWaitingNode {
		w := durableWaitingNode{
			NodeID: rapid.StringMatching(`[a-z]{1,8}`).Draw(t, "nodeID"),
			Suspension: dag.Suspension{
				Kind:           rapid.SampledFrom([]string{"", dag.SuspensionKindTimer, dag.SuspensionKindSignal, "other"}).Draw(t, "kind"),
				SignalType:     rapid.SampledFrom([]string{"", "agent.done", "human_judgment.responded", "foo", "foo.bar"}).Draw(t, "signalType"),
				CorrelationKey: rapid.SampledFrom([]string{"", "1:run:a", "1:run:b"}).Draw(t, "correlationKey"),
			},
		}
		if rapid.Bool().Draw(t, "hasRequiredPayload") {
			w.RequiredPayload = rapid.SliceOfN(rapid.StringMatching(`[a-z]{1,4}`), 1, 4).Draw(t, "required")
		}
		if rapid.Bool().Draw(t, "hasDefaults") {
			w.DefaultOutputs = signalStringMapGen(false).Draw(t, "defaults")
		}
		return w
	})
}

// signalRequestGen produces signalRequests with non-empty payload values so
// the payload-wins invariant isn't tripped by the status-defaulting branch.
func signalRequestGen() *rapid.Generator[signalRequest] {
	return rapid.Custom[signalRequest](func(t *rapid.T) signalRequest {
		s := signalRequest{
			IdempotencyKey: rapid.StringMatching(`[a-z0-9]{4,8}`).Draw(t, "idempotencyKey"),
			SignalType:     rapid.SampledFrom([]string{"", "agent.done", "agent.done.retry", "human_judgment.responded", "foo", "foo.bar", "foobar"}).Draw(t, "signalType"),
			CorrelationKey: rapid.SampledFrom([]string{"", "1:run:a", "1:run:b", "1:run:c"}).Draw(t, "correlationKey"),
		}
		if rapid.Bool().Draw(t, "hasPayload") {
			s.Payload = signalStringMapGen(true).Draw(t, "payload")
		}
		return s
	})
}

// signalStringMapGen builds a non-empty string map. When nonEmptyValues is
// true every value is guaranteed non-empty (used for signal payloads to avoid
// colliding with the status-defaulting branch inside outputsForSignal).
func signalStringMapGen(nonEmptyValues bool) *rapid.Generator[map[string]string] {
	return rapid.Custom[map[string]string](func(t *rapid.T) map[string]string {
		n := rapid.IntRange(1, 4).Draw(t, "size")
		m := make(map[string]string, n)
		for i := 0; i < n; i++ {
			key := rapid.StringMatching(`[a-z]{1,6}`).Draw(t, "key")
			valPattern := `[a-z0-9]{0,8}`
			if nonEmptyValues {
				valPattern = `[a-z0-9]{1,8}`
			}
			m[key] = rapid.StringMatching(valPattern).Draw(t, "val")
		}
		return m
	})
}

// TestWaitingMatchesSignalInvariants: a match result implies Kind == "signal",
// correlation compatibility, and signal-type equality or "<waiting>." prefix.
func TestWaitingMatchesSignalInvariants(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		w := waitingNodeGen().Draw(t, "waiting")
		s := signalRequestGen().Draw(t, "signal")
		ok := waitingMatchesSignal(w, s)

		if w.Kind != "signal" && ok {
			t.Fatalf("matched non-signal waiting (Kind=%q): %+v %+v", w.Kind, w, s)
		}
		if !ok {
			return
		}
		if w.CorrelationKey != "" && s.CorrelationKey != w.CorrelationKey {
			t.Fatalf("matched with mismatched correlation: waiting=%q signal=%q", w.CorrelationKey, s.CorrelationKey)
		}
		if w.SignalType != "" && s.SignalType != w.SignalType && !strings.HasPrefix(s.SignalType, w.SignalType+".") {
			t.Fatalf("matched with non-prefix signalType: waiting=%q signal=%q", w.SignalType, s.SignalType)
		}
	})
}

// TestValidateSignalForWaitingRequiresPayload: err exactly when any required
// key is missing or blank on the signal payload.
func TestValidateSignalForWaitingRequiresPayload(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		w := waitingNodeGen().Draw(t, "waiting")
		s := signalRequestGen().Draw(t, "signal")
		err := validateSignalForWaiting(w, s)

		anyRequired := false
		missing := false
		for _, key := range w.RequiredPayload {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			anyRequired = true
			if strings.TrimSpace(s.Payload[key]) == "" {
				missing = true
				break
			}
		}
		if !anyRequired && err != nil {
			t.Fatalf("unexpected error with no required keys: err=%v", err)
		}
		if missing && err == nil {
			t.Fatalf("missing required payload did not error: waiting=%+v signal=%+v", w, s)
		}
		if anyRequired && !missing && err != nil {
			t.Fatalf("all required keys present but errored: %v", err)
		}
	})
}

// TestOutputsForSignalInvariants: the merged output map always has a non-empty
// status, preserves non-empty payload keys verbatim, and is not aliased to
// waiting.DefaultOutputs.
func TestOutputsForSignalInvariants(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		w := waitingNodeGen().Draw(t, "waiting")
		s := signalRequestGen().Draw(t, "signal")
		outputs := outputsForSignal(w, s)

		if outputs == nil {
			t.Fatal("outputs is nil")
		}
		if outputs["status"] == "" {
			t.Fatalf("status is empty: %+v", outputs)
		}
		for k, v := range s.Payload {
			if outputs[k] != v {
				t.Fatalf("payload[%q]=%q not preserved in outputs (got %q)", k, v, outputs[k])
			}
		}

		// Sanity: mutating the returned map must not leak back into the
		// waiting entry's DefaultOutputs.
		if w.DefaultOutputs != nil {
			outputs["__sentinel__"] = "xyz"
			if _, leaked := w.DefaultOutputs["__sentinel__"]; leaked {
				t.Fatal("outputs map aliases waiting.DefaultOutputs")
			}
		}
	})
}
