---
name: blueprint-author
description: Author, review, or debug blueprints for the question-market resolution engine. Use when drafting a new resolution workflow, adapting a docs/examples/ pattern, fixing a blueprint-validation error (NO_RETURN_PATH, NON_RETURN_LEAF, RETURN_HAS_OUTGOING, BACK_EDGE_MISSING_MAX_TRAVERSALS, EDGE_UNKNOWN_OUTPUT_KEY, etc.), deciding which node type fits a step, or looking up an executor's config/outputs. Triggers on "write a blueprint", "author blueprint", "resolution blueprint", "draft blueprint", "fix blueprint validation", "what does node X output", "design a resolution workflow".
---

# Blueprint Author

Guide for composing valid blueprints for the resolution engine. Leverages
the in-repo node catalog, self-describing executor schemas, and edge-
reference diagnostics.

## When to Use

- **write a blueprint**, **draft blueprint**, **design a resolution workflow**
- Adapting one of `docs/examples/` to a different market shape
- Diagnosing a validation error or warning
- Looking up a node's config fields or output keys
- Deciding between `llm_call` vs `agent_loop`, `wait` vs `await_signal`, etc.

## Authoring Workflow

1. **Pick a decision pattern.** Start from an existing example when the
   shape matches, otherwise compose from primitives. Every blueprint
   ends in one or more `return` nodes whose payload `{status, ...}` the
   indexer interprets.
   - Single canonical source → `api_fetch` → `return` (success) / `return` (cancelled)
   - Model judgment → `llm_call` or `agent_loop` (resolution mode) → `return`
   - Human-in-the-loop → `await_signal` with `timeout_outputs` fallback → `return`
   - Parallel judges → `map` over inline `agent_loop` + aggregator → `return`
   - Deferred decision → `wait` (defer mode) → `return` with `{"status": "deferred"}`
   - Meta/generated blueprint → `agent_loop` → `validate_blueprint` → `gadget` → `return`

2. **Look up every node type in the catalog.** The catalog lists config
   fields, output keys, host constraints, and a minimal example for each
   type. Do not guess outputs.

3. **Wire edges with CEL conditions against `results.<nodeID>.<outputKey>`.**
   Every executor sets `status`, so `results.<node>.status == 'success'` is
   the canonical success gate. Check the catalog for additional outputs
   (e.g. `results.judge.outcome`, `results.fetch.outcome`). Caller-supplied
   inputs are accessed as `inputs.<key>` — bare keys are not resolved.

4. **Enforce the return-path rule.** Every reachable non-`return` node
   must lead (forward or via a back-edge loop) to a `return`. Every
   `return` node must be a leaf. Add a failure-branch `return` for every
   non-terminal node so error/inconclusive paths still terminate.

5. **Mark back-edges.** Any cycle needs `"max_traversals": N` on the back-
   edge; the validator rejects it otherwise.

6. **Validate before running.** Either:
   - Call `ValidateResolutionBlueprint(bp, raw)` in Go.
   - Include a `validate_blueprint` node fed from a context key, then
     branch on `valid == "true"`.
   Fix all errors. `EDGE_UNKNOWN_OUTPUT_KEY` warnings ship with a
   "Did you mean X?" suggestion — trust them.

## Core Invariants

- **Two hosts, one contract.** Top-level runs go through the durable
  manager; `map`/`gadget`/agent-tool children go through the in-memory
  engine. Suspension-capable nodes (`await_signal`, async `agent_loop`,
  long/`defer` `wait`) are rejected in child blueprints.
- **Status is universal.** `status == "success"` vs `"failed"` is on every
  executor. Gate edges on status first, then on node-specific outputs.
- **Return payload shape.** `return` requires a JSON object with a
  non-empty `status` string. Convention: `success` carries `outcome`,
  `cancelled`/`failed` carry `reason`, `deferred` carries `reason`. The
  engine doesn't enforce these — they're caller-side conventions read by
  the indexer.
- **First-fire wins.** When multiple `return` nodes are wired on parallel
  branches, only the first to complete is kept. Later returns on still-
  running branches are dropped silently and their branches are marked
  skipped.
- **History is engine-provided.** Back-edge iterations append the previous
  iteration's outputs into `results.<node>.history` as a JSON array. Safe to
  reference in CEL without declaring (e.g. `results.fetch.history.size() > 0`).

## Common Pitfalls

- Forgetting a failure-branch `return` → `NON_RETURN_LEAF` or `NO_RETURN_PATH`.
- Wiring outgoing edges from a `return` node → `RETURN_HAS_OUTGOING`.
- Using `agent_loop.async=true` inside a gadget/map child → blueprint rejected.
- Typo in edge identifier (`results.fetch.outcom`) → `EDGE_UNKNOWN_OUTPUT_KEY` warning.
- Back-edge without `max_traversals` → `BACK_EDGE_MISSING_MAX_TRAVERSALS`.
- Forgetting a `return` in a `map` / `gadget` / `blueprint`-tool child;
  the parent will see the strict "blueprint completed without firing
  return" failure at runtime.

## Key References

Absolute URLs so this skill works when installed outside the repo too.

| Topic | Location |
| --- | --- |
| All node types: config, outputs, host constraints, examples | [docs/node-catalog.md](https://github.com/andsav/question-market-resolution-engine/blob/main/docs/node-catalog.md) |
| Execution model, two-host semantics, suspension | [docs/execution-architecture.md](https://github.com/andsav/question-market-resolution-engine/blob/main/docs/execution-architecture.md) |
| Domain blueprints (agent panel, human sign-off, etc.) | [docs/examples/](https://github.com/andsav/question-market-resolution-engine/tree/main/docs/examples) |
| Executor source of truth (ConfigSchema, OutputKeys) | [executors/](https://github.com/andsav/question-market-resolution-engine/tree/main/executors) |
| Top-level validator | [blueprint_validation.go](https://github.com/andsav/question-market-resolution-engine/blob/main/blueprint_validation.go) |
