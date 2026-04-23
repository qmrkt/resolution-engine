# Bug Bounty Report: resolution-engine & question.market

Hello team,

I have performed an extensive investigation into the core user flows of the `question.market` platform and the `resolution-engine` backend. Below are five deterministic issues I identified that can significantly obstruct user interaction or compromise service stability.

---

## 1. [Critical] Service Crash via Unbounded Loop Traversals
**Impacted Flow:** Market viewing & Payout claims
**Severity:** Critical (OOM Vector)

**Description:**
The engine validation logic in `blueprint_validation.go` currently checks if a back-edge has `MaxTraversals` set to a positive integer, but it fails to enforce a reasonable upper bound. An attacker can define a loop with a massive traversal limit (e.g., 1,000,000). Since every iteration snapshots results into the persistent state, this leads to a linear memory and disk growth until the service crashes with an Out-of-Memory (OOM) error.

**Reproduction Steps:**
Submit a blueprint with a simple loop where `max_traversals` is set to `1,000,000`.

---

## 2. [High] Memory Exhaustion in Map Executor
**Impacted Flow:** Market viewing (Resolution state)
**Severity:** High

**Description:**
In `executors/map.go`, the `MaxItems` parameter is not capped by a system-level limit. When processing large arrays, the executor encodes all results into a single output string. For extremely large item sets, the `json.Marshal` operation will exhaust available memory, causing a deterministic crash of the worker processing that run.

**Reproduction Steps:**
Submit a blueprint using a `map` node with `max_items` set to `1,000,000`.

---

## 3. [Medium-High] Callback Outbox Blocking (HOL DoS)
**Impacted Flow:** Payout claiming
**Severity:** Medium-High (Service Degradation)

**Description:**
The callback delivery mechanism in `durable_manager.go` is strictly sequential and synchronous. If several markets have slow callback servers (hitting the 10s timeout), they will block the entire outbox queue. This creates a Head-of-Line (HOL) blocking scenario where legitimate, fast callbacks for other markets are delayed for minutes or hours.

**Reproduction Steps:**
1. Trigger 10 runs with slow callback URLs (9s delay).
2. Trigger an 11th run with a fast callback.
3. Observe the massive delay in the 11th callback's delivery.

---

## 4. [Medium] Permanent Resolution Failure on Minor Payload Errors
**Impacted Flow:** Payout claiming
**Severity:** Medium (UX Blocking)

**Description:**
The `normalizeReturnPayload` function is overly rigid. If an AI-generated blueprint or a dynamic gadget returns a JSON that lacks the specific `status` field (e.g., it returns `result` instead), the entire resolution fails. Without a fallback path, that market becomes permanently "bricked" in an unresolved state.

**Reproduction Steps:**
Deploy a blueprint where the final `return` node points to a key containing valid JSON but missing the `status` field.

---

## 5. [Medium] UI Confusion: Duplicate Outcome Names
**Impacted Flow:** Market creation & Trading
**Severity:** Medium (Visual/Logical Confusion)

**Description:**
The frontend allows users to create markets with identical outcome names (e.g., "Yes" and "Yes"). While the backend might use unique IDs, the end-user has no way to distinguish between positions in the orderbook, effectively bricking the trading experience for that specific market.

**Reproduction Steps:**
1. Navigate to the "Ask a Question" page.
2. Enter "Yes" for both Outcome 1 and Outcome 2.
3. The UI allows you to proceed and create the market without warning.

### Visual Evidence
Below is a recording demonstrating the duplicate outcomes bug in the frontend:

![Duplicate Outcomes Demo](evidence.webp)

---

I hope these findings are helpful for the bounty program! Let me know if you need any further logs or reproduction scripts.
