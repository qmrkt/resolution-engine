package dag

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// TestEvalCELCache_TypeSignatureKeyedCorrectly is the central stale-cache
// regression test. Same expression, same ident set, but the context value's
// type shifts across calls — the compiled program must NOT be reused with a
// mismatched declaration.
//
// The expression `fetch._runs.size() > 0` produces ident `fetch._runs` (the
// `.size` tail is stripped as a CEL method). When `fetch._runs` is a JSON
// list, it's declared as list<dyn> and `.size()` runs on the list. When the
// key is absent, it's declared as empty string and `.size()` runs on the
// string — returning 0.
func TestEvalCELCache_TypeSignatureKeyedCorrectly(t *testing.T) {
	expr := `fetch._runs.size() > 0`

	// Phase 1: list type with content → true.
	ctx1 := NewContext(map[string]string{
		"fetch._runs": `[{"x":1}]`,
	})
	ok, err := EvalCondition(expr, ctx1)
	if err != nil {
		t.Fatalf("phase 1: %v", err)
	}
	if !ok {
		t.Fatal("phase 1: expected true")
	}

	// Phase 2: key absent → falls through to empty-string declaration.
	// "".size() == 0, so result is false. If the cache keyed only on the
	// expression text, this would try to eval the list-typed program with
	// no activation for `fetch._runs` — either erroring or misbehaving.
	ctx2 := NewContext(nil)
	ok, err = EvalCondition(expr, ctx2)
	if err != nil {
		t.Fatalf("phase 2: %v", err)
	}
	if ok {
		t.Fatal("phase 2: expected false")
	}

	// Phase 3: list type again, different content → true. Confirms the
	// list-typed program is still reachable after a string-typed detour.
	ctx3 := NewContext(map[string]string{
		"fetch._runs": `[{"a":1},{"b":2},{"c":3}]`,
	})
	ok, err = EvalCondition(expr, ctx3)
	if err != nil {
		t.Fatalf("phase 3: %v", err)
	}
	if !ok {
		t.Fatal("phase 3: expected true")
	}
}

// TestEvalCELCache_MapVsScalarForSameIdent covers the second staleness axis:
// same ident, value shape changes between map and scalar string.
func TestEvalCELCache_MapVsScalarForSameIdent(t *testing.T) {
	// Case A: judge.details is a JSON map — expression accesses .outcome.
	ctx := NewContext(map[string]string{
		"judge.details": `{"outcome":"0","confidence":"high"}`,
	})
	ok, err := EvalCondition(`judge.details.outcome == "0"`, ctx)
	if err != nil {
		t.Fatalf("map case: %v", err)
	}
	if !ok {
		t.Fatal("map case: expected true")
	}

	// Case B: judge.details is a plain string. The expression then needs
	// `judge.details.outcome` as its own declared variable (scalar prefix
	// is not declared structurally). The ident resolves to empty string,
	// so the comparison to "0" is false.
	ctx2 := NewContext(map[string]string{
		"judge.details": "opaque-string",
	})
	ok, err = EvalCondition(`judge.details.outcome == "0"`, ctx2)
	if err != nil {
		t.Fatalf("scalar case: %v", err)
	}
	if ok {
		t.Fatal("scalar case: expected false (judge.details.outcome declared as empty)")
	}
}

// TestEvalCELCache_RepeatedCallsConsistent — same expression, same context
// shape, value changes across calls must produce correct per-call results.
// Exercises the hot path repeatedly to make sure the activation map isn't
// aliased or reused across calls.
func TestEvalCELCache_RepeatedCallsConsistent(t *testing.T) {
	expr := `status == "completed"`
	for i := 0; i < 200; i++ {
		want := i%2 == 0
		value := "pending"
		if want {
			value = "completed"
		}
		ctx := NewContext(map[string]string{"status": value})
		got, err := EvalCondition(expr, ctx)
		if err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		if got != want {
			t.Fatalf("iter %d: got %v, want %v (value=%q)", i, got, want, value)
		}
	}
}

// TestEvalCELCache_ConcurrentMixedShapes hammers the cache from many
// goroutines with different type signatures for the same expression. A
// correctness bug in the cache key would surface as wrong results or
// panics under -race.
func TestEvalCELCache_ConcurrentMixedShapes(t *testing.T) {
	expr := `fetch._runs.size() >= 1`

	const workers = 32
	const iters = 100
	var wrong atomic.Int64
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				var (
					ctx  *Context
					want bool
				)
				switch (id + i) % 3 {
				case 0:
					ctx = NewContext(map[string]string{"fetch._runs": `[{"a":1}]`})
					want = true
				case 1:
					ctx = NewContext(map[string]string{"fetch._runs": `[]`})
					want = false
				case 2:
					ctx = NewContext(nil) // absent → empty string → "".size()==0
					want = false
				}
				got, err := EvalCondition(expr, ctx)
				if err != nil {
					t.Errorf("worker %d iter %d: %v", id, i, err)
					return
				}
				if got != want {
					wrong.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()

	if n := wrong.Load(); n > 0 {
		t.Fatalf("%d concurrent evals produced wrong results", n)
	}
}

// TestEvalCELPlan_DeterministicIdentifiers ensures the plan cache never
// returns differently-ordered identifier slices for the same expression —
// varSig must be stable for the compiled-program cache to work.
func TestEvalCELPlan_DeterministicIdentifiers(t *testing.T) {
	expr := `fetch.status == "ok" && judge.outcome != "inconclusive" && fetch._runs.size() > 0`

	first := getOrBuildPlan(expr)
	// Call many times — same pointer after the first LoadOrStore.
	for i := 0; i < 50; i++ {
		again := getOrBuildPlan(expr)
		if len(again.identifiers) != len(first.identifiers) {
			t.Fatalf("iter %d: identifier count changed", i)
		}
		for j, id := range again.identifiers {
			if id != first.identifiers[j] {
				t.Fatalf("iter %d: identifier %d changed %q -> %q", i, j, first.identifiers[j], id)
			}
		}
	}

	// Identifiers must be sorted (the compiled-program cache key depends
	// on varSig order matching the declaration order).
	for i := 1; i < len(first.identifiers); i++ {
		if first.identifiers[i-1] > first.identifiers[i] {
			t.Fatalf("identifiers not sorted at %d: %q > %q",
				i, first.identifiers[i-1], first.identifiers[i])
		}
	}
}

// TestEvalCELPlan_CandidatesIncludePrefixes guards the contract between the
// plan cache and buildActivation: every prefix that might be consulted for
// structural resolution must be present in the candidates slice so that
// GetMulti returns it in the raw map.
func TestEvalCELPlan_CandidatesIncludePrefixes(t *testing.T) {
	plan := getOrBuildPlan(`a.b.c.d == "x"`)

	// Identifier is `a.b.c.d`; candidates must include it plus every dotted prefix.
	wantCandidates := map[string]bool{
		"a.b.c.d": false,
		"a.b.c":   false,
		"a.b":     false,
		"a":       false,
	}
	for _, c := range plan.candidates {
		if _, known := wantCandidates[c]; !known {
			t.Errorf("unexpected candidate %q", c)
			continue
		}
		wantCandidates[c] = true
	}
	for c, seen := range wantCandidates {
		if !seen {
			t.Errorf("missing candidate %q", c)
		}
	}
}

// TestContextGetMulti_MissingVsEmpty confirms GetMulti preserves the
// present-but-empty vs missing distinction that buildActivation relies on
// when deciding whether to declare an ident or fall through to prefix
// resolution.
func TestContextGetMulti_MissingVsEmpty(t *testing.T) {
	ctx := NewContext(map[string]string{
		"present.empty":  "",
		"present.scalar": "x",
	})

	got := ctx.GetMulti([]string{"present.empty", "present.scalar", "absent"})

	if v, ok := got["present.empty"]; !ok || v != "" {
		t.Errorf("present.empty: got (%q,%v), want (%q,true)", v, ok, "")
	}
	if v, ok := got["present.scalar"]; !ok || v != "x" {
		t.Errorf("present.scalar: got (%q,%v), want (%q,true)", v, ok, "x")
	}
	if _, ok := got["absent"]; ok {
		t.Error("absent: got present, want missing")
	}
}

// TestContextGetMulti_NilAndEmpty — edge cases that callers may hit when
// evaluating constant expressions like `true` with no identifiers.
func TestContextGetMulti_NilAndEmpty(t *testing.T) {
	ctx := NewContext(nil)
	if got := ctx.GetMulti(nil); got != nil {
		t.Errorf("nil keys: got %v, want nil", got)
	}
	if got := ctx.GetMulti([]string{}); got != nil {
		t.Errorf("empty keys: got %v, want nil", got)
	}

	var nilCtx *Context
	if got := nilCtx.GetMulti([]string{"k"}); got != nil {
		t.Errorf("nil context: got %v, want nil", got)
	}
}

// TestEvalCEL_PrefixResolvesListAndMap exercises the prefix branch of
// buildActivation to guarantee it behaves identically to the former
// full-snapshot implementation.
func TestEvalCEL_PrefixResolvesListAndMap(t *testing.T) {
	tests := []struct {
		name       string
		values     map[string]string
		expression string
		want       bool
	}{
		{
			name:       "list prefix",
			values:     map[string]string{"fetch._runs": `[{"s":"ok"},{"s":"ok"}]`},
			expression: `fetch._runs.size() == 2`,
			want:       true,
		},
		{
			name:       "map prefix",
			values:     map[string]string{"judge.details": `{"outcome":"0"}`},
			expression: `judge.details.outcome == "0"`,
			want:       true,
		},
		{
			name:       "nested map prefix",
			values:     map[string]string{"a.b": `{"c":{"d":"e"}}`},
			expression: `a.b.c.d == "e"`,
			want:       true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewContext(tc.values)
			got, err := EvalCondition(tc.expression, ctx)
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if got != tc.want {
				t.Fatalf("%s: got %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

// TestEvalCEL_NoIdentifiers ensures constant expressions still work after
// caching refactor — no ident lookups, no activation map entries.
func TestEvalCEL_NoIdentifiers(t *testing.T) {
	ctx := NewContext(nil)
	for _, tc := range []struct {
		expr string
		want bool
	}{
		{"true", true},
		{"false", false},
		{"1 + 2 == 3", true},
		{`"hello" == "hello"`, true},
	} {
		got, err := EvalCondition(tc.expr, ctx)
		if err != nil {
			t.Fatalf("%q: %v", tc.expr, err)
		}
		if got != tc.want {
			t.Fatalf("%q: got %v, want %v", tc.expr, got, tc.want)
		}
	}
}

// TestEvalCEL_ConcurrentIdenticalShape specifically targets the
// LoadOrStore race in planCache — many goroutines, first call, identical
// expression. All must observe the same plan.
func TestEvalCEL_ConcurrentIdenticalShape(t *testing.T) {
	// Unique expression so it cannot be in the cache from a prior test.
	expr := `concurrent_test_unique_expr.x == "probe_` + fmt.Sprint(1) + `"`

	const goroutines = 64
	var wg sync.WaitGroup
	pointers := make([]*exprPlan, goroutines)

	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			pointers[idx] = getOrBuildPlan(expr)
		}(i)
	}
	close(start)
	wg.Wait()

	// All goroutines must converge on the same stored plan pointer.
	for i := 1; i < goroutines; i++ {
		if pointers[i] != pointers[0] {
			t.Fatalf("goroutine %d got a different plan pointer than goroutine 0", i)
		}
	}
}
