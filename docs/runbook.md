# Runbook

It is 3am, the nightly run is red, and you are the one awake.

This document is for whoever is on call, who may not be you. Write it so a
tired stranger can follow it.

## Triage, in order

**1. Is it the platform or the data?**
```bash
docker compose ps                       # anything unhealthy?
docker compose logs --tail=100 airflow-scheduler
```
A container that is down is a platform problem. Everything healthy but the
task failed is a data or code problem.

**2. Which run, and which file?**
```sql
SELECT run_id, source_file, status, rows_read, rows_loaded, rows_rejected, error_message
FROM staging.pipeline_runs ORDER BY started_at DESC LIMIT 10;
```

**3. Read the structured log for that run.** Every line carries `run_id`:
```bash
docker compose logs airflow-scheduler | grep '"run_id":"<the-run-id>"'
```

## Telling the two apart

| Symptom | Most likely | First move |
|---|---|---|
| `TransientError` after 3 attempts | infrastructure | check MinIO/Postgres health |
| `PermanentError: 404` | file vanished mid-run | check the archive bucket, it may have been processed already |
| `ValidationError: missing column` | **schema drift upstream** | stop. Talk to the source owner before touching code. |
| High `rows_rejected` | data quality drop upstream | query `staging.rejected_rows` for the reasons |
| Volume-drop alert | partial upstream extract | do NOT re-run blindly; you may load an incomplete day |
| Task stuck 'running' | hung call with no timeout | that is a bug; file it |

## Safe re-run

The upsert is idempotent, so re-running a file is safe by design.

```bash
# one file: move it back from archive to raw, then trigger
docker compose exec airflow-scheduler airflow dags trigger sales_ingestion
```

**Do not re-run when** the volume-drop check fired, because the source may
have delivered a partial extract and you would load an incomplete day on top
of a complete one. Confirm with the source owner first.

## Escalate when

- The source schema changed: the source team owns that, not you
- The warehouse is unreachable: platform team
- The same failure has recurred three nights running: stop patching, fix the
  cause in daylight
- Anything you do not understand after 30 minutes. Waking someone is cheaper
  than a wrong fix at 3am.

## After any incident

- Add a test that would have caught it. Every bug leaves behind a test.
- Add an ADR if a design assumption turned out wrong.
- Update this runbook with what you actually did.

## Manual Metabase setup (fallback)

`make metabase-setup` does this for you (see `scripts/setup_metabase.py`). If
it ever breaks against a newer Metabase release, do it by hand: Admin ->
Databases -> Add a database -> Postgres, host `postgres`, database
`analytics`, user `metabase_ro`. Then New Question -> Native query, and paste
each of these onto its own card:

```sql
-- Revenue trend over time
SELECT date_trunc('day', order_date)::date AS day, SUM(revenue) AS revenue
FROM marts.sales_orders GROUP BY 1 ORDER BY 1;

-- Revenue by product category
SELECT product_category, SUM(revenue) AS revenue
FROM marts.sales_orders GROUP BY 1 ORDER BY revenue DESC;

-- Top 10 customers by revenue
SELECT customer_id, SUM(revenue) AS revenue
FROM marts.sales_orders GROUP BY 1 ORDER BY revenue DESC LIMIT 10;

-- Rejected rows per day
SELECT day, rejected_rows FROM marts.v_daily_rejections ORDER BY day;
```

## Turning on Slack alerts

Both the DAG (`on_failure_callback`) and the CI workflow (`notify-failure`
job) already check for a webhook and post to it - they just need one to
exist:

1. In Slack: add an "Incoming Webhook" to the channel you want alerts in.
2. Locally: set `SLACK_WEBHOOK_URL` in `.env`.
3. In CI: add `SLACK_WEBHOOK_URL` as a repository secret (Settings -> Secrets
   and variables -> Actions).

Neither one requires a code change - both are silent no-ops (log-only) until
the variable/secret exists.

## Protecting `main`

Settings -> Branches -> add a rule for `main`: require the CI status checks
(`lint`, `unit-tests`, `integration-tests`, `dag-validation`) to pass, and
require a pull request before merging. Combined with the `test` deploy
environment's required reviewer, no change reaches a deployed environment
without both a robot and a human agreeing first.
