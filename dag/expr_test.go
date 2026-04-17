package dag

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// Reusing the same expression with different input types must not hit a stale CEL program.
func TestEvalCELCache_TypeSignatureKeyedCorrectly(t *testing.T) {
	expr := `inputs.fetch_runs.size() > 0`

	// List input.
	ctx1 := NewInvocationFromInputs(map[string]string{
		"fetch_runs": `[{"x":1}]`,
	})
	ok, err := EvalCondition(expr, ctx1)
	if err != nil {
		t.Fatalf("phase 1: %v", err)
	}
	if !ok {
		t.Fatal("phase 1: expected true")
	}

	// Missing input.
	ctx2 := NewInvocationFromInputs(nil)
	ok, err = EvalCondition(expr, ctx2)
	if err != nil {
		t.Fatalf("phase 2: %v", err)
	}
	if ok {
		t.Fatal("phase 2: expected false")
	}

	// List input again.
	ctx3 := NewInvocationFromInputs(map[string]string{
		"fetch_runs": `[{"a":1},{"b":2},{"c":3}]`,
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
	// Case A: inputs.judge_details is a JSON map — expression accesses .outcome.
	ctx := NewInvocationFromInputs(map[string]string{
		"judge_details": `{"outcome":"0","confidence":"high"}`,
	})
	ok, err := EvalCondition(`inputs.judge_details.outcome == "0"`, ctx)
	if err != nil {
		t.Fatalf("map case: %v", err)
	}
	if !ok {
		t.Fatal("map case: expected true")
	}

	// Case B: inputs.judge_details is a plain string. The expression then
	// needs `inputs.judge_details.outcome` as its own declared variable
	// (scalar prefix is not declared structurally). The ident resolves to
	// empty string, so the comparison to "0" is false.
	ctx2 := NewInvocationFromInputs(map[string]string{
		"judge_details": "opaque-string",
	})
	ok, err = EvalCondition(`inputs.judge_details.outcome == "0"`, ctx2)
	if err != nil {
		t.Fatalf("scalar case: %v", err)
	}
	if ok {
		t.Fatal("scalar case: expected false (inputs.judge_details.outcome declared as empty)")
	}
}

// TestEvalCELCache_RepeatedCallsConsistent — same expression, same context
// shape, value changes across calls must produce correct per-call results.
// Exercises the hot path repeatedly to make sure the activation map isn't
// aliased or reused across calls.
func TestEvalCELCache_RepeatedCallsConsistent(t *testing.T) {
	expr := `inputs.status == "completed"`
	for i := 0; i < 200; i++ {
		want := i%2 == 0
		value := "pending"
		if want {
			value = "completed"
		}
		ctx := NewInvocationFromInputs(map[string]string{"status": value})
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
	expr := `inputs.fetch_runs.size() >= 1`

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
					ctx  *Invocation
					want bool
				)
				switch (id + i) % 3 {
				case 0:
					ctx = NewInvocationFromInputs(map[string]string{"fetch_runs": `[{"a":1}]`})
					want = true
				case 1:
					ctx = NewInvocationFromInputs(map[string]string{"fetch_runs": `[]`})
					want = false
				case 2:
					ctx = NewInvocationFromInputs(nil) // absent → empty string → "".size()==0
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
	expr := `inputs.fetch_status == "ok" && inputs.judge_outcome != "inconclusive" && inputs.fetch_runs.size() > 0`

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
	plan := getOrBuildPlan(`inputs.a.b.c.d == "x"`)

	// Identifier is `inputs.a.b.c.d`; candidates must include it plus every dotted prefix.
	wantCandidates := map[string]bool{
		"inputs.a.b.c.d": false,
		"inputs.a.b.c":   false,
		"inputs.a.b":     false,
		"inputs.a":       false,
		"inputs":         false,
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

// Present-but-empty and missing inputs must still behave differently.
func TestEvalCEL_PresentVsMissingViaInvocation(t *testing.T) {
	inv := NewInvocationFromInputs(map[string]string{
		"present.empty":  "",
		"present.scalar": "x",
	})

	// Present-but-empty: declared as a string variable; comparison succeeds.
	ok, err := EvalCondition(`inputs.present.empty == ""`, inv)
	if err != nil {
		t.Fatalf("present.empty eval: %v", err)
	}
	if !ok {
		t.Fatal("inputs.present.empty == \"\" expected true")
	}

	// Present scalar: equality with the literal succeeds.
	ok, err = EvalCondition(`inputs.present.scalar == "x"`, inv)
	if err != nil {
		t.Fatalf("present.scalar eval: %v", err)
	}
	if !ok {
		t.Fatal("inputs.present.scalar == \"x\" expected true")
	}

	// Missing: comparison to a non-empty string is false (CEL returns the
	// empty fallthrough; lenient map suppresses the no-such-key error).
	ok, err = EvalCondition(`inputs.absent == "x"`, inv)
	if err != nil {
		t.Fatalf("absent eval: %v", err)
	}
	if ok {
		t.Fatal("inputs.absent == \"x\" expected false")
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
			values:     map[string]string{"fetch_runs": `[{"s":"ok"},{"s":"ok"}]`},
			expression: `inputs.fetch_runs.size() == 2`,
			want:       true,
		},
		{
			name:       "map prefix",
			values:     map[string]string{"judge.details": `{"outcome":"0"}`},
			expression: `inputs.judge.details.outcome == "0"`,
			want:       true,
		},
		{
			name:       "nested map prefix",
			values:     map[string]string{"a.b": `{"c":{"d":"e"}}`},
			expression: `inputs.a.b.c.d == "e"`,
			want:       true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewInvocationFromInputs(tc.values)
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
	ctx := NewInvocationFromInputs(nil)
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
