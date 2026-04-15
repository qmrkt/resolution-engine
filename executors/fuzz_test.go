package executors

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/question-market/resolution-engine/dag"
	"pgregory.net/rapid"
)

// --- Byte-level fuzz targets (go test -fuzz) ---

// FuzzExtractJSON throws arbitrary strings at the LLM response JSON extractor.
// Property: must never panic. If input contains valid JSON, extractJSON should
// return valid JSON (or at minimum not crash).
func FuzzExtractJSON(f *testing.F) {
	f.Add(`{"outcome_index": 0, "confidence": "high", "reasoning": "clear evidence"}`)
	f.Add("```json\n{\"outcome_index\": 1}\n```")
	f.Add(`Some text before {"outcome_index": 0, "confidence": "low", "reasoning": "unclear"} and after`)
	f.Add(`no json here at all`)
	f.Add(`{{{`)
	f.Add(`}}}`)
	f.Add(`{"a": {"b": {"c": 1}}}`)
	f.Add(``)
	f.Add(`{}`)
	f.Add(`{"key": "value with \n newlines and \"escapes\""}`)
	f.Add("```\n{}\n```")
	f.Add(`multiple {"a":1} objects {"b":2} in one string`)
	f.Add(`{unterminated`)
	f.Add(`"just a string"`)
	f.Add(`[1, 2, 3]`)

	f.Fuzz(func(t *testing.T, input string) {
		result := extractJSON(input)
		// If the input was valid JSON, the result should be valid JSON too
		if json.Valid([]byte(input)) {
			if !json.Valid([]byte(result)) {
				// extractJSON may trim to a sub-object; only flag if
				// the whole input was a JSON object and the result isn't valid
				var obj map[string]interface{}
				if json.Unmarshal([]byte(input), &obj) == nil && !json.Valid([]byte(result)) {
					t.Errorf("valid JSON input produced invalid JSON output: %q -> %q", input, result)
				}
			}
		}
	})
}

// FuzzJSONPathExtract fuzzes the dot-notation JSON path extractor.
// Property: must never panic on arbitrary JSON + path combinations.
func FuzzJSONPathExtract(f *testing.F) {
	f.Add(`{"data": {"price": "70000"}}`, "data.price")
	f.Add(`{"a": 1}`, "a")
	f.Add(`{"a": {"b": {"c": "deep"}}}`, "a.b.c")
	f.Add(`{}`, "missing")
	f.Add(`{"a": 1}`, "")
	f.Add(`not json`, "any.path")
	f.Add(`{"a": [1,2,3]}`, "a")
	f.Add(`null`, "x")
	f.Add(`{"": "empty key"}`, "")
	f.Add(`{"a.b": "dotted key"}`, "a.b")

	f.Fuzz(func(t *testing.T, data string, path string) {
		_, _ = jsonPathExtract([]byte(data), path)
	})
}

// FuzzTrimCodeFence tests the code fence trimming helper.
func FuzzTrimCodeFence(f *testing.F) {
	f.Add("```json\n{}\n```")
	f.Add("```\ntext\n```")
	f.Add("no fence")
	f.Add("``````")
	f.Add("```json```")

	f.Fuzz(func(t *testing.T, input string) {
		_ = trimCodeFence(input)
	})
}

// --- Property-based tests (rapid) ---

// TestExtractJSONProperties verifies that extractJSON produces valid JSON when
// given input containing at least one valid JSON object.
func TestExtractJSONProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a valid JSON object
		outcome := rapid.IntRange(0, 10).Draw(t, "outcome")
		confidence := rapid.SampledFrom([]string{"high", "medium", "low"}).Draw(t, "confidence")
		reasoning := rapid.StringMatching(`[a-zA-Z0-9 .,!?]{0,100}`).Draw(t, "reasoning")

		obj := map[string]interface{}{
			"outcome_index": outcome,
			"confidence":    confidence,
			"reasoning":     reasoning,
		}
		jsonBytes, err := json.Marshal(obj)
		if err != nil {
			t.Fatal(err)
		}

		// Optionally wrap in noise
		prefix := rapid.SampledFrom([]string{
			"",
			"Here is my analysis:\n",
			"```json\n",
			"Based on the evidence, ",
		}).Draw(t, "prefix")

		suffix := rapid.SampledFrom([]string{
			"",
			"\n```",
			"\nEnd of response.",
		}).Draw(t, "suffix")

		input := prefix + string(jsonBytes) + suffix
		result := extractJSON(input)

		// The result must be valid JSON
		if !json.Valid([]byte(result)) {
			t.Fatalf("extractJSON produced invalid JSON: input=%q result=%q", input, result)
		}

		// The result must contain the same outcome_index
		var parsed map[string]interface{}
		if err := json.Unmarshal([]byte(result), &parsed); err != nil {
			t.Fatalf("cannot unmarshal result: %v", err)
		}
		idx, ok := parsed["outcome_index"]
		if !ok {
			t.Fatal("extracted JSON missing outcome_index")
		}
		if int(idx.(float64)) != outcome {
			t.Fatalf("outcome_index mismatch: got %v, want %d", idx, outcome)
		}
	})
}

// TestJSONPathExtractProperties verifies that jsonPathExtract returns the
// correct value for generated nested objects.
func TestJSONPathExtractProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a leaf value
		leaf := rapid.StringMatching(`[a-z0-9]{1,20}`).Draw(t, "leaf")

		// Generate a path depth 1-4
		depth := rapid.IntRange(1, 4).Draw(t, "depth")
		keys := make([]string, depth)
		for i := 0; i < depth; i++ {
			keys[i] = rapid.StringMatching(`[a-z]{1,8}`).Draw(t, "key")
		}

		// Build nested object
		var obj interface{} = leaf
		for i := depth - 1; i >= 0; i-- {
			obj = map[string]interface{}{keys[i]: obj}
		}

		data, err := json.Marshal(obj)
		if err != nil {
			t.Fatal(err)
		}

		path := keys[0]
		for i := 1; i < depth; i++ {
			path += "." + keys[i]
		}

		result, err := jsonPathExtract(data, path)
		if err != nil {
			t.Fatalf("jsonPathExtract(%s, %q) failed: %v", data, path, err)
		}
		if result != leaf {
			t.Fatalf("jsonPathExtract got %q, want %q", result, leaf)
		}
	})
}

// TestParseJudgmentResultProperties verifies that valid LLM JSON responses
// produce expected output structure.
func TestParseJudgmentResultProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		outcomeIndex := rapid.IntRange(0, 5).Draw(t, "outcome")
		confidence := rapid.SampledFrom([]string{"high", "medium", "low"}).Draw(t, "confidence")
		reasoning := rapid.StringMatching(`[a-zA-Z0-9 ]{1,50}`).Draw(t, "reasoning")

		judgment := map[string]interface{}{
			"outcome_index": outcomeIndex,
			"confidence":    confidence,
			"reasoning":     reasoning,
		}
		raw, _ := json.Marshal(judgment)

		result := parseJudgmentResult(string(raw), dag.TokenUsage{}, nil)

		if result.Outputs["status"] != "success" {
			t.Fatalf("expected success, got status=%q error=%q for input=%s",
				result.Outputs["status"], result.Outputs["error"], raw)
		}
		if result.Outputs["outcome"] != fmt.Sprintf("%d", outcomeIndex) {
			t.Fatalf("outcome mismatch: got %q, want %q", result.Outputs["outcome"], fmt.Sprintf("%d", outcomeIndex))
		}
	})
}
