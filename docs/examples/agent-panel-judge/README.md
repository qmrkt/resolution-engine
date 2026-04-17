# Agent Panel Judge

Use this for higher-stakes agent-assisted markets where a single opaque judge would be too hard to audit.

The panel separates concerns:

- `rules_agent`: interprets market wording and ambiguity.
- `evidence_agent`: gathers and summarizes evidence.
- `skeptic_agent`: checks for unresolved issues and alternate interpretations.
- `judge_agent`: makes the final structured resolution decision.

```mermaid
flowchart LR
  rules["Rules Agent<br/>agent_loop"]
  evidence["Evidence Agent<br/>agent_loop"]
  skeptic["Skeptic Agent<br/>agent_loop"]
  judge["Final Judge<br/>agent_loop"]
  submit["Submit<br/>return"]
  cancel["Cancel<br/>return"]

  rules --> skeptic
  evidence --> skeptic
  skeptic --> judge
  judge -->|"resolved outcome"| submit
  judge -->|"inconclusive or failed"| cancel
```

