# Blueprint Examples

These examples are starting points for prediction-market resolution workflows. Each folder contains:

- `blueprint.json`: a blueprint using the current engine node vocabulary.
- `README.md`: when to use the pattern, required inputs, and a Mermaid diagram.

The examples intentionally use placeholder URLs and context keys. Market creation should bind those placeholders to concrete sources, outcome mappings, and trust policies.

## Examples

| Example | Best for |
| --- | --- |
| [`objective-api-resolution`](objective-api-resolution/) | Markets with one canonical machine-readable source |
| [`verify-official-result`](verify-official-result/) | Sports/event markets with an agent-facing official-result tool |
| [`redundant-source-consensus`](redundant-source-consensus/) | Objective markets where multiple sources should agree |
| [`evidence-dossier-agent`](evidence-dossier-agent/) | Public-web evidence gathering with a single tool-using agent |
| [`agent-panel-judge`](agent-panel-judge/) | Higher-stakes resolution with separate specialist agents and a judge |
| [`ai-brief-human-signoff`](ai-brief-human-signoff/) | Human-judged markets where an agent prepares the evidence brief |
| [`dispute-challenge-review`](dispute-challenge-review/) | Challenge/dispute path review after a proposed resolution |
