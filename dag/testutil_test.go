package dag

import "encoding/json"

// resultValue reads `<nodeID>.<field>` from a RunState's Results.
func resultValue(run *RunState, nodeID, field string) string {
	if run == nil || run.Results == nil {
		return ""
	}
	v, _ := run.Results.Get(nodeID, field)
	return v
}

// resultHistoryJSON returns the back-edge history of a node as a JSON
// array string. Empty string when there is no history.
func resultHistoryJSON(run *RunState, nodeID string) string {
	if run == nil || run.Results == nil {
		return ""
	}
	history := run.Results.History(nodeID)
	if len(history) == 0 {
		return ""
	}
	data, err := json.Marshal(history)
	if err != nil {
		return ""
	}
	return string(data)
}
