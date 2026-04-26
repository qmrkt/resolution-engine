package main

import (
	"sync"
	"time"
)

// durableRunIndex is an in-memory projection of the run store's hot-path
// metadata. Every successful persist updates it via Upsert; queries hit
// RAM instead of walking runs/*.json.
//
// The index holds the *minimum* data needed to answer the hot questions
// without loading a full record:
//   - "is there an active run for app X?" (Submit duplicate check)
//   - "how many queued runs?" (Submit queue cap)
//   - "how many non-terminal runs?" (ActiveCount, /health)
//   - "which runs have a due-timer waiting right now?" (wakeDueRuns)
//   - "which terminal runs still need callback delivery?" (deliverCallbacks)
//   - "what run ID services app X for a scoped signal?" (findSignalTarget)
//
// Callers that need the full record still call loadRun(runID) — the index
// just narrows the candidate set to O(1) lookup or a cheap per-run
// predicate, replacing the O(H) directory scan.
//
// Consistency: Upsert is gated on the record's Revision. A stale Upsert
// (older rev arriving after a newer one) is discarded, so concurrent
// persists that land at the store in revision order can't be undone by
// an out-of-order index write.
type durableRunIndex struct {
	mu   sync.RWMutex
	runs map[string]*durableIndexEntry
}

type durableIndexEntry struct {
	AppID             int
	Status            RunStatus
	Revision          int64
	CallbackURL       string
	CallbackDelivered bool
	// NextCallbackAtUnix is the backoff deadline for a pending callback, or
	// 0 when delivery is eligible immediately. Parsed once at Upsert so the
	// callback poll can filter without re-parsing strings.
	NextCallbackAtUnix int64
	// EarliestDueTimer is the smallest ResumeAtUnix across the record's
	// waiting entries, or 0 if the record has no timer-bound waiting.
	EarliestDueTimer int64
}

func newDurableRunIndex() *durableRunIndex {
	return &durableRunIndex{runs: make(map[string]*durableIndexEntry)}
}

// Upsert folds a record's current state into the index. Older revisions
// are ignored so out-of-order post-saveRun calls can't clobber newer
// state. Safe to call without the store lock held.
func (ix *durableRunIndex) Upsert(record *durableRunRecord) {
	if record == nil {
		return
	}
	runID := record.Request.RunID
	if runID == "" {
		return
	}
	entry := buildIndexEntry(record)

	ix.mu.Lock()
	defer ix.mu.Unlock()
	if existing, ok := ix.runs[runID]; ok && existing.Revision > entry.Revision {
		return
	}
	ix.runs[runID] = entry
}

func buildIndexEntry(record *durableRunRecord) *durableIndexEntry {
	entry := &durableIndexEntry{
		AppID:             record.Request.AppID,
		Status:            record.Result.Status,
		Revision:          record.Revision,
		CallbackURL:       record.Request.CallbackURL,
		CallbackDelivered: record.CallbackDelivered,
	}
	if record.NextCallbackAt != "" {
		if next, err := time.Parse(time.RFC3339Nano, record.NextCallbackAt); err == nil {
			entry.NextCallbackAtUnix = next.Unix()
		}
	}
	var earliest int64
	for _, waiting := range record.Checkpoint.Waiting {
		if waiting.ResumeAtUnix > 0 && (earliest == 0 || waiting.ResumeAtUnix < earliest) {
			earliest = waiting.ResumeAtUnix
		}
	}
	entry.EarliestDueTimer = earliest
	return entry
}

// filterRunIDs scans the index under the read lock and returns every run
// ID whose entry satisfies pred. Every read-side query is expressed as a
// predicate, so there is only one locking and iteration pattern.
func (ix *durableRunIndex) filterRunIDs(pred func(*durableIndexEntry) bool) []string {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	var out []string
	for runID, entry := range ix.runs {
		if pred(entry) {
			out = append(out, runID)
		}
	}
	return out
}

func (ix *durableRunIndex) countRuns(pred func(*durableIndexEntry) bool) int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	count := 0
	for _, entry := range ix.runs {
		if pred(entry) {
			count++
		}
	}
	return count
}

// ActiveCount returns the number of non-terminal runs.
func (ix *durableRunIndex) ActiveCount() int {
	return ix.countRuns(func(e *durableIndexEntry) bool {
		return !isTerminalStatus(e.Status)
	})
}

// QueuedCount returns the number of runs whose status is currently queued.
func (ix *durableRunIndex) QueuedCount() int {
	return ix.countRuns(func(e *durableIndexEntry) bool {
		return e.Status == RunStatusQueued
	})
}

// DueTimerRunIDs returns run IDs with at least one due-timer waiting
// entry as of the now timestamp.
func (ix *durableRunIndex) DueTimerRunIDs(now int64) []string {
	return ix.filterRunIDs(func(e *durableIndexEntry) bool {
		if isTerminalStatus(e.Status) {
			return false
		}
		return e.EarliestDueTimer > 0 && e.EarliestDueTimer <= now
	})
}

// PendingCallbackRunIDs returns terminal run IDs with an undelivered
// callback whose backoff deadline (if any) has elapsed.
func (ix *durableRunIndex) PendingCallbackRunIDs(nowUnix int64) []string {
	return ix.filterRunIDs(func(e *durableIndexEntry) bool {
		if !isTerminalStatus(e.Status) || e.CallbackDelivered || e.CallbackURL == "" {
			return false
		}
		return e.NextCallbackAtUnix == 0 || e.NextCallbackAtUnix <= nowUnix
	})
}

// ActiveRunIDsByApp returns every non-terminal run for the given app.
// Used by findSignalTarget when the caller scopes by app_id only, and by
// activeRunForApp when checking for a duplicate submission.
func (ix *durableRunIndex) ActiveRunIDsByApp(appID int) []string {
	return ix.filterRunIDs(func(e *durableIndexEntry) bool {
		return e.AppID == appID && !isTerminalStatus(e.Status)
	})
}

// allActiveRunIDs returns every non-terminal run. Used by findSignalTarget
// when the caller has no app/run scoping — still O(active) instead of the
// previous O(all-history).
func (ix *durableRunIndex) allActiveRunIDs() []string {
	return ix.filterRunIDs(func(e *durableIndexEntry) bool {
		return !isTerminalStatus(e.Status)
	})
}
