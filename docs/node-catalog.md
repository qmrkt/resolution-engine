# Node Catalog

Every node type registered by [`runner.go`](../runner.go), with its config
schema, outputs, and host constraints.

This doc is intended for agents and humans authoring blueprints. It is the
user-facing counterpart to the authoritative schemas each executor declares
via [`dag.ConfigSchemaProvider`](../dag/engine.go) and
[`dag.OutputKeyProvider`](../dag/engine.go). When the doc and the schema
disagree, the schema wins — regenerate the relevant section.

For execution semantics (sync vs. durable, suspension lifecycle,
cancellation) see [`execution-architecture.md`](./execution-architecture.md).

## Quick reference

| Type | Purpose | Suspends | Sub-blueprint safe | Terminates run |
| --- | --- | --- | --- | --- |
| [`api_fetch`](#api_fetch) | HTTP fetch + JSONPath extract + outcome mapping | No | Yes | No |
| [`llm_call`](#llm_call) | Single-shot LLM call for evidence → outcome | No | Yes | No |
| [`agent_loop`](#agent_loop) | Tool-using agent loop; optional async dispatch | Only when `async: true` | Yes (when `async: false`) | No |
| [`return`](#return) | Emit a JSON value and short-circuit the run | No | Yes | **Yes** |
| [`await_signal`](#await_signal) | Park until an external signal or timeout | Yes | No | No |
| [`wait`](#wait) | Sleep inline (short) or park on a timer (long) | Only when long / defer-mode | Yes (short sleeps) | No |
| [`cel_eval`](#cel_eval) | Evaluate CEL expressions over the shared context | No | Yes | No |
| [`map`](#map) | Run an inline child blueprint over each batch of items | No | Yes | No |
| [`gadget`](#gadget) | Run a child blueprint with input/output wiring | No | Yes | No |
| [`validate_blueprint`](#validate_blueprint) | Validate a blueprint JSON provided via context | No | Yes | No |

A `return` node emits a JSON value (the run's final result) and
short-circuits everything else: in-flight siblings are cancelled, parked
waits are cleared, and the run terminates. Every valid blueprint must
contain at least one `return` reachable from a root, and every reachable
non-`return` node must lead (forward or via a back-edge loop) to a
`return`. The first `return` to complete wins; later returns on parallel
branches are dropped.

## Blueprint wiring basics

- **Lookup namespaces.** Templates and CEL expressions resolve through three
  namespaced surfaces: `inputs.<key>` (caller inputs), `results.<node>.<field>`
  (node outputs), and `results.<node>.history` (back-edge iteration history
  as a JSON array). Bare keys are not resolved.
- **Edges.** `{"from": "N", "to": "M", "condition": "results.N.status == 'success'", "max_traversals": 1}`.
  Conditions are CEL; `max_traversals` is required on any back-edge.
- **Interpolation.** String config values pass through `Invocation.Interpolate`,
  so `{{inputs.market.question}}` and `{{results.N.output}}` both work.
- **Status.** Every executor sets `status` to `"success"` or `"failed"`.
  Terminals also set `status = "success"`. Use `node.status == 'success'`
  as the most common edge condition.

## Node types

### api_fetch

Fetch a URL, optionally extract a value via JSONPath, and map it to an
outcome index.

**Config**

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `url` | string | yes | Absolute URL. Localhost / private IPs blocked outside tests. |
| `method` | string | no | One of `GET`, `POST`, `PUT`, `PATCH`, `DELETE`. Default `GET`. |
| `headers` | map[string]string | no | |
| `body` | string | no | Interpolation applies. |
| `basic_auth` | `{username, password}` | no | |
| `json_path` | string | no | Dot-notation into the response JSON. Empty returns the raw body. |
| `outcome_mapping` | map[string]string | no | Maps extracted value → outcome index (as a string). |
| `timeout_seconds` | int | no | |

**Outputs**: `status`, `outcome`, `raw`, `json_path_value`, `error`,
`http_status`.

**Example**

```json
{
  "id": "fetch",
  "type": "api_fetch",
  "config": {
    "url": "https://example.com/winner",
    "json_path": "result.winner",
    "outcome_mapping": {"Yes": "0", "No": "1"}
  }
}
```

### llm_call

Single-shot LLM call. Supply a prompt referencing prior-node outputs;
receive a structured judgment.

**Config**

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `prompt` | string | yes | Supports `{{inputs.X}}` / `{{results.<node>.<field>}}` interpolation. |
| `provider` | string | no | `anthropic` \| `openai` \| `google`. |
| `model` | string | no | Provider-specific model id. |
| `timeout_seconds` | int | no | |
| `web_search` | bool | no | Anthropic server-side web search. Ignored elsewhere. |
| `allowed_outcomes_key` | string | no | Namespaced lookup path holding a JSON array of valid outcome labels; the executor rejects outcomes outside the range. |

**Outputs**: `status`, `outcome`, `reasoning`, `confidence`, `raw`,
`citations_json`, `error`.

**Example**

```json
{
  "id": "judge",
  "type": "llm_call",
  "config": {
    "provider": "anthropic",
    "prompt": "Evidence: {{results.fetch.raw}}\n\nDecide: {{inputs.market.question}}",
    "allowed_outcomes_key": "inputs.market.outcomes_json"
  }
}
```

### agent_loop

Tool-using agent loop. The model plans and calls tools over multiple steps
until it emits a final output. Supports three output modes and an optional
async dispatch.

**Config (selected)**

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `prompt` | string | yes | |
| `provider`, `model`, `system_prompt`, `timeout_seconds` | — | no | As in `llm_call`. |
| `output_mode` | string | no | `text` (default), `structured` (requires `output_tool.parameters`), `resolution` (canonical outcome/confidence/reasoning). |
| `output_tool` | object | sometimes | JSON schema for `structured` mode. |
| `tools` | array | no | Each element is a builtin id or an inline sub-blueprint tool. |
| `context_allowlist` | string[] | no | Restricts what the agent's tools may read. |
| `enable_dynamic_blueprints` | bool | no | Allows the `run_blueprint` builtin. |
| `async` | bool | no | When true, dispatches the loop on a goroutine and returns a signal suspension. **Only valid under the durable engine.** |

**Outputs** (varies by `output_mode`): `status`, `error`, `outcome`,
`summary`, `text`, `output_json`, `raw`, `resolution_status`, `confidence`,
`reasoning`, `citations_json`, `citations_count`, `tool_calls_count`,
`tool_calls_json`, `steps_json`, `transcript_tail`.

**Suspension**

- `async: false` (default): completes in place; safe in any host.
- `async: true`: suspends via signal; fails blueprint validation in any
  sub-blueprint context (gadget child, map body, agent blueprint tool).

**Example (resolution mode)**

```json
{
  "id": "agent",
  "type": "agent_loop",
  "config": {
    "prompt": "Resolve {{inputs.market.question}}",
    "output_mode": "resolution",
    "allowed_outcomes_key": "inputs.market.outcomes_json",
    "tools": [{"name": "source_fetch", "kind": "builtin", "builtin": "source_fetch"}]
  }
}
```

### return

Terminal. Emits a JSON value as the run's final result and short-circuits
the rest of the run. The engine writes the value to `RunState.Return` /
`RunResult.Return`; consumers (the indexer, gadget/map/blueprint-tool
wrappers) interpret the payload however they like.

**Config** — exactly one of:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `value` | object | xor `from_key` | Literal JSON object to return. String leaves are interpolated using `{{inputs.X}}` / `{{results.<node>.<field>}}`. |
| `from_key` | string | xor `value` | Namespaced lookup path whose value (a JSON-object string) is returned verbatim. |

The resolved payload **must** be a JSON object with a non-empty top-level
`status` string. Anything else is free for the consumer to interpret. By
convention: `success` carries an `outcome`, `cancelled`/`failed` carry a
`reason`, `deferred` carries a `reason`. The engine does not enforce or
inspect those — it only stores the payload.

**Outputs**: `status`. The full payload travels on `RunState.Return`, not
on the node's outputs map.

**Examples**

```json
// Success — outcome from an upstream judge.
{"id": "submit", "type": "return", "config": {"value": {"status": "success", "outcome": "{{results.judge.outcome}}", "confidence": "{{results.judge.confidence}}"}}}

// Cancellation branch.
{"id": "cancel", "type": "return", "config": {"value": {"status": "cancelled", "reason": "evidence inconclusive"}}}

// Defer — push the decision back to the caller.
{"id": "defer", "type": "return", "config": {"value": {"status": "deferred", "reason": "human did not respond in time"}}}

// Pass through a JSON value an upstream node already shaped.
{"id": "passthrough", "type": "return", "config": {"from_key": "results.agent.output_json"}}
```

### await_signal

Park the run until a matching external signal is delivered, or the timeout
fires.

**Config**

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `signal_type` | string | yes | Prefix matching: `"foo"` matches `"foo"` and `"foo.bar"`. |
| `correlation_key` | string | no | Empty or `"auto"` derives `"<app_id>:<run_id>:<node_id>"`. |
| `timeout_seconds` | int | no | Default 172800 (2 days). |
| `required_payload` | string[] | no | Signal is rejected if these keys are missing or blank. |
| `default_outputs` | map[string]string | no | Merged under the delivered payload on resume. |
| `timeout_outputs` | map[string]string | no | Emitted on timeout. Default `{"status": "timeout"}`. |
| `reason` | string | no | Trace-only. |

**Outputs**: the merge of `default_outputs` + signal payload (or
`timeout_outputs` on timeout). `status` is always present.

**Host constraint**: only valid under the durable engine. Rejected in any
sub-blueprint context.

**Example**

```json
{
  "id": "judge",
  "type": "await_signal",
  "config": {
    "signal_type": "human_judgment.responded",
    "required_payload": ["outcome"],
    "default_outputs": {"status": "responded"},
    "timeout_seconds": 3600,
    "timeout_outputs": {"status": "timeout", "outcome": "inconclusive"}
  }
}
```

### wait

Sleep. Short waits run inline and return synchronously; long waits return a
timer suspension. Defer mode waits until a wall-clock anchor in
`market.*`.

**Config**

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `duration_seconds` | int | no | Required when `mode` is `sleep`. |
| `mode` | string | no | `sleep` (default) or `defer`. |
| `start_from` | string | sometimes | Anchor under `market.*` for defer mode. Must not be `now`. |
| `max_inline_seconds` | int | no | `<0` forces suspension for any positive duration; `0` uses the default inline cap (20s); `>0` is an explicit cap, validated `≤ 300` by blueprint validation. |

**Outputs**: `status`, `waited`, `mode` (sleep mode); adds `start_from`,
`anchor_ts`, `ready_at`, `remaining_seconds` in defer mode.

**Host constraint**: short sleeps that stay within the inline cap are safe
in sub-blueprints. Longer waits and defer mode fail sub-blueprint
validation.

### cel_eval

Evaluate a map of CEL expressions over the shared context. Each key becomes
an output.

**Config**

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `expressions` | map[string]string | yes | At least one entry. Keys become output names; values are CEL source. |

**Outputs**: `status`, plus one string-valued output per expression key.

**Example**

```json
{
  "id": "should_retry",
  "type": "cel_eval",
  "config": {
    "expressions": {
      "retry": "fetch.status != 'success' && fetch._runs.size() < 3"
    }
  }
}
```

### map

Run an inline child blueprint over each batch of items from a
context-provided JSON array.

**Config (selected)**

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `items_key` | string | yes | Context key holding a JSON array. |
| `inline` | Blueprint | yes | Child blueprint. Must not contain suspension-capable nodes. |
| `batch_size` | int | no | `0` = one batch with all items. |
| `batch_input_key`, `batch_index_input_key`, `batch_start_index_input_key`, `batch_end_index_input_key`, `batch_item_count_input_key` | string | no | Per-batch context keys the child sees. |
| `max_concurrency` | int? | no | `nil` / `0` = sequential. |
| `on_error` | string | no | `fail` (default) or `continue`. |
| `max_items`, `max_depth` | int | no | Guards. |
| `per_batch_timeout_seconds` | int | no | |
| `input_mappings` | map[string]string | no | Extra parent → child inputs. |

The child blueprint is required to fire a `return` per batch. The map
collects each batch's `RunState.Return` value into the per-batch
`mapBatchResult.Return` field.

**Outputs**: `status`, `results`, `total_items`, `total_batches`,
`completed_batches`, `failed_batches`, `skipped_batches`, `first_error`.

`results` is a JSON array of `mapBatchResult` envelopes
`{batch_index, batch_start_index, batch_end_index, batch_item_count,
items, status, return, error, usage}`. The `return` field carries the
child blueprint's emitted JSON value.

### gadget

Run a child blueprint with input/output wiring. The child is dispatched
through the in-memory engine, so it inherits the no-suspension rule.

**Config**

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `blueprint_json`, `blueprint_json_key`, `inline` | — | one of | Exactly one source. |
| `input_mappings` | map[string]string | no | `child_input_key → parent_context_key`. |
| `timeout_seconds` | int | no | |
| `max_depth` | int | no | Recursion guard across nested gadgets. Default `1`. |
| `dynamic_blueprint_policy` | object | no | Overrides allowed node types, depth, and token budgets for runtime-supplied children. |

The child blueprint is required to fire a `return`. Its emitted payload
is exposed verbatim as the gadget node's `return_json` output (a JSON
string).

**Outputs**: `status`, `error`, `run_status`, `child_run_id`, `return_json`.

### validate_blueprint

Validate a blueprint JSON string already staged in the shared context.
Commonly chained after an `agent_loop` that drafts a blueprint, and before
a `gadget` that would execute it.

**Config**

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `blueprint_json_key` | string | yes | Context key holding the candidate blueprint JSON. |

**Outputs**: `status`, `valid`, `issue_count`, `issues_json`,
`issues_text`, `blueprint_json`, `first_issue_code`, `first_issue_message`,
`first_issue_target`.

## Quickstart primitives

These patterns use only the primitives above. Pair with
[`docs/examples/`](./examples/) for domain-oriented resolution patterns
(multi-agent judge, human-in-the-loop sign-off, redundant sources, etc.).

**One API fetch → return** — the minimum valid blueprint.

```json
{
  "id": "api",
  "version": 1,
  "nodes": [
    {"id": "fetch", "type": "api_fetch", "config": {"url": "https://example.com/v1/winner", "json_path": "result.winner", "outcome_mapping": {"Yes": "0", "No": "1"}}},
    {"id": "submit", "type": "return", "config": {"value": {"status": "success", "outcome": "{{results.fetch.outcome}}"}}},
    {"id": "cancel", "type": "return", "config": {"value": {"status": "cancelled", "reason": "api did not return a winner"}}}
  ],
  "edges": [
    {"from": "fetch", "to": "submit", "condition": "results.fetch.status == 'success'"},
    {"from": "fetch", "to": "cancel", "condition": "results.fetch.status != 'success'"}
  ]
}
```

**LLM judge with timeout** — bound the LLM call and fall back on failure.

```json
{
  "id": "judge",
  "version": 1,
  "nodes": [
    {"id": "judge", "type": "llm_call", "config": {"prompt": "Resolve: {{inputs.market.question}}", "timeout_seconds": 30, "allowed_outcomes_key": "inputs.market.outcomes_json"}},
    {"id": "submit", "type": "return", "config": {"value": {"status": "success", "outcome": "{{results.judge.outcome}}"}}},
    {"id": "cancel", "type": "return", "config": {"value": {"status": "cancelled", "reason": "judge did not succeed"}}}
  ],
  "edges": [
    {"from": "judge", "to": "submit", "condition": "results.judge.status == 'success' && results.judge.outcome != 'inconclusive'"},
    {"from": "judge", "to": "cancel", "condition": "results.judge.status != 'success' || results.judge.outcome == 'inconclusive'"}
  ]
}
```

**Human-in-the-loop with timeout fallback** — wait for an external signal,
fall through to a defer if the human doesn't respond.

```json
{
  "id": "human",
  "version": 1,
  "nodes": [
    {"id": "judge", "type": "await_signal", "config": {"signal_type": "human_judgment.responded", "required_payload": ["outcome"], "default_outputs": {"status": "responded"}, "timeout_outputs": {"status": "timeout"}, "timeout_seconds": 3600}},
    {"id": "submit", "type": "return", "config": {"value": {"status": "success", "outcome": "{{results.judge.outcome}}"}}},
    {"id": "defer", "type": "return", "config": {"value": {"status": "deferred", "reason": "human did not respond in time"}}}
  ],
  "edges": [
    {"from": "judge", "to": "submit", "condition": "results.judge.status == 'responded'"},
    {"from": "judge", "to": "defer", "condition": "results.judge.status == 'timeout'"}
  ]
}
```

**Retry loop** — re-run a fetch a bounded number of times using `wait` +
`max_traversals`.

```json
{
  "id": "retry",
  "version": 1,
  "nodes": [
    {"id": "fetch", "type": "api_fetch", "config": {"url": "https://example.com/v1/result", "json_path": "result", "outcome_mapping": {"yes": "0", "no": "1"}}},
    {"id": "backoff", "type": "wait", "config": {"duration_seconds": 30, "max_inline_seconds": -1}},
    {"id": "submit", "type": "return", "config": {"value": {"status": "success", "outcome": "{{results.fetch.outcome}}"}}},
    {"id": "cancel", "type": "return", "config": {"value": {"status": "cancelled", "reason": "fetch never succeeded"}}}
  ],
  "edges": [
    {"from": "fetch", "to": "backoff", "condition": "results.fetch.status != 'success'"},
    {"from": "backoff", "to": "fetch", "max_traversals": 3},
    {"from": "fetch", "to": "submit", "condition": "results.fetch.status == 'success'"},
    {"from": "fetch", "to": "cancel", "condition": "results.fetch.status != 'success' && results.fetch.history.size() >= 3"}
  ]
}
```

**Fan out over items** — a `map` node batching per child blueprint. Each
batch's child blueprint must end in a `return`.

```json
{
  "id": "fanout",
  "version": 1,
  "nodes": [
    {
      "id": "score",
      "type": "map",
      "config": {
        "items_key": "inputs.market.claims_json",
        "batch_size": 5,
        "max_concurrency": 4,
        "inline": {
          "version": 1,
          "nodes": [
            {"id": "rate", "type": "cel_eval", "config": {"expressions": {"score": "'1'"}}},
            {"id": "out", "type": "return", "config": {"value": {"status": "success", "score": "{{results.rate.score}}"}}}
          ],
          "edges": [{"from": "rate", "to": "out"}]
        }
      }
    },
    {"id": "submit", "type": "return", "config": {"value": {"status": "success", "completed_batches": "{{results.score.completed_batches}}"}}},
    {"id": "cancel", "type": "return", "config": {"value": {"status": "cancelled", "reason": "map failed"}}}
  ],
  "edges": [
    {"from": "score", "to": "submit", "condition": "results.score.status == 'success'"},
    {"from": "score", "to": "cancel", "condition": "results.score.status != 'success'"}
  ]
}
```

## See also

- [`docs/execution-architecture.md`](./execution-architecture.md) — the
  two-host model, suspension semantics, cancellation architecture.
- [`docs/examples/`](./examples/) — full-blueprint examples for
  prediction-market resolution patterns.
- [`dag/engine.go`](../dag/engine.go) — `NodeExecutor`,
  `ConfigSchemaProvider`, `OutputKeyProvider`, `SuspendableExecutor`,
  `CancellableExecutor` interfaces.
- [`executors/`](../executors) — the executors themselves. Each
  `ConfigSchema()` method is the authoritative source for the types listed
  above.
