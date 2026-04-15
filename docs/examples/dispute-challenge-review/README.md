# Dispute Challenge Review

Use this as a dispute path after someone challenges a proposed resolution. The agent is not merely resolving from scratch; it evaluates whether the challenge undermines the proposed outcome.

The reviewer can:

- reaffirm the proposed outcome,
- cancel when the market is unresolvable or invalid,
- escalate to a human market admin.

```mermaid
flowchart LR
  review["Challenge Reviewer<br/>agent_loop"]
  admin["Admin Fallback<br/>await_signal"]
  submitReview["Submit Reviewed Outcome<br/>submit_result"]
  submitAdmin["Submit Admin Outcome<br/>submit_result"]
  cancelReview["Cancel From Review<br/>cancel_market"]
  cancelAdmin["Cancel From Admin Timeout<br/>cancel_market"]

  review -->|"reaffirm"| submitReview
  review -->|"cancel"| cancelReview
  review -->|"escalate or failed"| admin
  admin -->|"admin outcome"| submitAdmin
  admin -->|"timeout or inconclusive"| cancelAdmin
```
