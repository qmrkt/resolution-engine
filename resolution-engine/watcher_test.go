package main

import (
	"encoding/json"
	"testing"
)

func TestMarketInfoOutcomesUnmarshalAcceptsDirectArray(t *testing.T) {
	var market MarketInfo
	if err := json.Unmarshal([]byte(`{
		"appId": 19,
		"status": 2,
		"question": "Will it rain?",
		"outcomes": ["Yes", "No"]
	}`), &market); err != nil {
		t.Fatalf("unmarshal market: %v", err)
	}

	if string(market.Outcomes) != `["Yes","No"]` {
		t.Fatalf("expected canonical outcomes payload, got %q", string(market.Outcomes))
	}
}

func TestMarketInfoOutcomesUnmarshalAcceptsLegacyJSONString(t *testing.T) {
	var market MarketInfo
	if err := json.Unmarshal([]byte(`{
		"appId": 20,
		"status": 2,
		"question": "Will it snow?",
		"outcomes": "[\"Yes\",\"No\"]"
	}`), &market); err != nil {
		t.Fatalf("unmarshal market: %v", err)
	}

	if string(market.Outcomes) != `["Yes","No"]` {
		t.Fatalf("expected legacy outcomes payload to be preserved, got %q", string(market.Outcomes))
	}
}
