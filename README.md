# question.market blueprint engine

Async blueprint execution service for question.market.

This repo is intentionally scoped to execution only:
- accepts blueprint execution requests over HTTP
- runs DAG-based resolution logic
- exposes async run status and cancellation
- emits structured execution traces for observability

It does not:
- poll markets
- decide market lifecycle transitions
- submit on-chain transactions
- own durable orchestration state

Those concerns belong to the indexer/orchestrator in the monorepo.

## Architecture

A Go service (`resolution-engine/`) that:
- executes resolution blueprints as DAGs with conditional branching
- supports multiple executor types: LLM judge, API fetch, human judge, market evidence, creator/admin input
- returns structured terminal results (`propose`, `finalize_dispute`, `cancel_market`, `defer`, `none`)
- exposes async HTTP endpoints for submit/status/cancel
- emits trace snapshots to the indexer for observability

## Quick start

Requires Go 1.23+.

```bash
cd resolution-engine

# Dependencies
go mod download

# Required environment
export INDEXER_URL=http://localhost:3001
export LISTEN_PORT=3002
export ANTHROPIC_API_KEY=your_anthropic_api_key   # optional
export OPENAI_API_KEY=your_openai_api_key         # optional
export GOOGLE_API_KEY=your_google_api_key         # optional

# Optional auth
export ENGINE_CONTROL_TOKEN=your_engine_control_token
export ENGINE_CALLBACK_TOKEN=your_engine_callback_token
export TRACE_INGEST_TOKEN=your_trace_ingest_token

# Run
go run .

# Tests
go test ./...
```

## HTTP API

### POST /run
Submit a blueprint execution request.

Request body:
```json
{
  "app_id": 123,
  "blueprint_json": {"id":"bp","nodes":[],"edges":[]},
  "inputs": {"market_question":"Will BTC hit 150k?"},
  "blueprint_path": "main",
  "initiator": "indexer:status-transition",
  "callback_url": "http://indexer.local/markets/123/resolution-result"
}
```

Response:
```json
{
  "run_id": "...",
  "app_id": 123,
  "blueprint_path": "main",
  "status": "accepted",
  "action": "none"
}
```

### GET /runs/{run_id}
Returns live/ephemeral run state while the engine still retains the run.

### DELETE /runs/{run_id}
Requests cancellation of an in-flight run.

### GET /health
Returns service health and active run count.

## Protocol notes

- Engine run state is in-memory and ephemeral.
- The indexer is the durable source of truth for orchestration.
- Traces are observability only, not authoritative control-plane state.
- Terminal run results are retained only for a bounded TTL.

## Testing

This repo keeps strong execution-service tests:
- DAG engine tests under `resolution-engine/dag/`
- executor tests under `resolution-engine/executors/`
- async run manager tests in `run_manager_test.go`
- HTTP API tests in `server_test.go`
- trace lifecycle tests in `runner_test.go`

The richer full-stack lifecycle and orchestration tests belong in the monorepo.

## License

See [LICENSE](./LICENSE).
