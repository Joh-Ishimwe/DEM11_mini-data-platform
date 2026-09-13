# Roles and ownership

On this project you play every role at once. In a real team it splits like
this, and the rule behind the split is: **the team that writes the code owns
the checks on that code.**

| Role | Owns |
|---|---|
| Platform / DevOps | Reusable workflow templates, runners, secrets management, container registry, the deploy mechanism and rollback button. *Not your pipeline's tests.* |
| Data Engineer (you) | Everything about your pipeline's correctness: unit, integration, data quality and schema tests; the DAG; the runbook. You are paged when it breaks. |
| Tech Lead | The standard, not the plumbing: which checks are mandatory, the coverage floor, branch protection rules. Arbitrates "CI is too slow" vs "CI doesn't catch enough". |
| Analytics Engineer | Tests on the transformation and metric layer. |
| Data Analyst | The **definition of correct**. "Revenue excludes cancelled orders" is a business rule. You encode it; they confirm you encoded it right. |
| Security | Secret scanning, dependency scanning. Their checks are mandatory and cannot be disabled by feature teams. |
| SRE / On-call | Runtime health. They need your runbook. If you haven't written one, you are on call forever. |
| Product Owner | The SLA. "Fresh by 6am, 99% of days" is a business decision, and it decides whether a late run is an alert or a shrug. |

The phrase for the data engineer's row is **"you build it, you run it."** It
exists to stop engineers throwing untested code over a wall at an ops team.

## The three failure modes, all organisational

1. **"CI is the platform team's problem."** Engineers write shallow tests
   because they don't feel accountable for the signal. Green means nothing.
2. **Nobody owns the red build.** Everyone assumes someone else is on it.
   Three days later people merge over a broken main as normal. Fix: a named
   rule that a red `main` outranks everything, and whoever broke it fixes or
   reverts immediately.
3. **Flaky tests have no owner.** Everyone re-runs, nobody fixes, trust in
   the whole system erodes. Fix: assign flaky tests like bugs, with a name.

## On this repo

A `CODEOWNERS` file (see the repo root) maps paths to owners, and GitHub
auto-requests review from them. It turns "who is responsible?" from tribal
knowledge into something enforced.
