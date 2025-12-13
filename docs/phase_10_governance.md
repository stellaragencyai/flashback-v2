\# Phase 10 Governance (Multi-AI Ecosystem)



\## Non-negotiables

\- No agent may change live trading behavior without an explicit approval gate.

\- All promotions must be reversible.

\- Any anomaly triggers safe mode (halt or LEARN\_DRY), not “try harder”.



\## Allowed automated actions (SAFE)

\- Generate suggestions (policy thresholds, risk multipliers)

\- Generate reports (drift, performance, canary status)

\- Recommend promotions (never apply automatically)



\## Forbidden automated actions (REQUIRE HUMAN APPROVAL)

\- Changing automation\_mode to LIVE\_FULL

\- Increasing risk\_pct

\- Switching model version in production

\- Changing exit profile rules

\- Editing config/subaccounts.yaml or config/strategies.yaml in-place



\## Required safety artifacts

\- state/execution\_suspect.lock

\- state/canary\_hard\_lock

\- state/policy\_lock (prevents auto-apply)

\- models/versioned artifacts with meta json



\## Rollback rules

\- Any promotion must create:

&nbsp; - a before snapshot

&nbsp; - an after snapshot

&nbsp; - a rollback command

\- If performance deteriorates beyond thresholds:

&nbsp; - demote automatically

&nbsp; - lock further promotions

&nbsp; - alert via Telegram



\## Minimum observability

\- Every agent action writes an event record (JSONL)

\- Every promotion attempt records metrics + decision rationale



