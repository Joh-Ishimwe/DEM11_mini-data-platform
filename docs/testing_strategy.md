# Testing strategy

Written before the tests, not after. Deciding what to test is the hard part;
writing the assertion is the easy part.

## The two-variable problem

Ordinary software has one variable: the code. Correct code, correct
behaviour. Data pipelines have two.

| What breaks | Example | Caught where |
|---|---|---|
| **The code** | `clean_orders()` drops rows it shouldn't | CI, on the PR, before merge |
| **The data** | The vendor sends `"12.50"` instead of `12.50` | Runtime, at 2am, in production |

**CI can only prove your code is consistent with your assumptions. It can
never prove your assumptions match tomorrow's reality.** That single sentence
is why `validation.py` runs inside the pipeline and not only in tests.

## The tiers

| Tier | What it checks | Where it runs | Touches | Budget |
|---|---|---|---|---|
| Pre-commit | formatting, lint, no secrets, no raw data | your laptop | nothing | <5s |
| Unit | one pure function at a time | every PR | nothing | <5s |
| Data quality | expectations against messy samples | every PR | nothing | <30s |
| Integration | real Postgres, real MinIO | every PR | service containers | <5m |
| DAG validation | imports, cycles, owner, retries, timeout | every PR | Airflow only | <2m |
| E2E | all four hops, plus idempotency | master, tags, nightly | full stack | <20m |
| Runtime quality | real data, every run | production | live data | per run |

Pre-commit can be bypassed with `--no-verify`, so CI re-runs the same checks.
Fast feedback for you; enforcement for the team.

## What a green tick actually means

Mechanically: every step that ran exited with code 0. Nothing more. It is
worth exactly as much as the checks behind it.

Ways a green tick lies, all of them common:

1. The tests are vacuous (assert the function returns a DataFrame)
2. The tests don't cover the code that changed
3. A job was skipped and nobody noticed
4. The fixtures are too clean to resemble production
5. The check isn't required, so people merge red anyway
6. It tested the code, and the code was never the problem

**Self-test:** delete the deduplication step and re-run CI. If it stays
green, your green means nothing and you now know it. Worth doing once,
deliberately, so you trust the suite for the rest of the project.

## Fatal vs quarantine

| Check | Classification | Why |
|---|---|---|
| required column missing | FATAL | no sensible way to continue |
| types not coercible at all | FATAL | the file is not what we think it is |
| `order_id` null | quarantine | can't identify that order, others are fine |
| `order_id` duplicate | cleaning | dedupe before validation even sees it |
| `quantity <= 0` | quarantine | impossible, but only for that row |
| `unit_price < 0` | quarantine | same |
| `order_date` in the future | quarantine | data entry error |
| unknown category | *you decide, and document it* | ADR-011 |
| inconsistent country case | cleaning | normalise, never reject |
| `country` empty | accept | nullable by design |

Too fatal and a single typo kills the nightly load. Too lenient and you
silently ship half a dataset. This table is the whole judgement call.

## Rules

- No unit test touches the network, a database, or the clock
- Non-deterministic fixtures are banned: flaky tests train people to ignore red
- Every bug fixed leaves behind a test, so it can never return silently
- Test data must include the messy cases or the suite proves nothing
- Never commit real customer data as a fixture; synthesise it
