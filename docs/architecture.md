# Architecture

## Why each component exists

**MinIO as a landing zone, rather than loading CSVs straight into Postgres.**
The raw file is kept untouched. If your transformation logic turns out to be
wrong, you re-run from raw instead of going back to the source and asking for
the data again. It also decouples arrival from processing: files can land at
any time and the pipeline picks them up on its own schedule. Being
S3-compatible means the same `boto3` code works unchanged against AWS.

**Airflow as an orchestrator, not a processing engine.** Airflow decides what
runs, in what order, when, and what happens on failure. The actual data work
happens in pandas inside `src/`. Confusing the two, and doing heavy
processing inside Airflow tasks, is a common and expensive mistake.

**Postgres as the warehouse.** Small scale, relational data, familiar SQL,
and Metabase speaks to it natively. At 50 million rows a day this is the
first thing to replace.

**Metabase for the last mile.** Data nobody looks at has no value. It reads
`marts` only, through a read-only user, because a BI tool has no business
being able to write to the warehouse.

## Data flow

1. Generator writes a CSV into `raw/sales/`
2. `list_files` returns new keys
3. `process_file` runs once **per file** via dynamic task mapping, so one bad
   file fails only its own task instance rather than the whole run
4. Download → clean → validate → apply business rules → upsert → quarantine
   → archive
5. `summarise` aggregates counts and runs the volume-drop check
6. Metabase reads `marts.sales_orders`

## The GitHub Actions cron limitation

`schedule:` in Actions is not a real orchestrator. No dependency graph between
tasks, no backfills, no retry of a single failed task, best-effort scheduling
that can be delayed under load, and a hard per-job timeout.

Actions cron is fine for small, independent, failure-tolerant jobs. Reach for
Airflow (which is why it is here) or Dagster when you need task-level
dependencies, backfills, or per-task retries.

## Where this breaks at scale

| Pressure | First thing to break | Direction |
|---|---|---|
| 50M rows/day | pandas in memory; row-by-row upsert | Spark or DuckDB; COPY into a staging table then MERGE |
| Many files/minute | Airflow scheduler latency | event-driven trigger, or a streaming ingest |
| Many concurrent BI users | Postgres serving both writes and dashboards | a read replica, or a columnar warehouse |
| Many teams | one repo, one CI pipeline | reusable workflow templates owned by a platform team |

## Trust boundaries

- Metabase connects as `metabase_ro`: SELECT on `marts` only
- Secrets come from `.env` locally and repository secrets in CI, never from code
- `__repr__` is overridden on both config classes so a password can never be
  printed into a log line. Printing a whole config object is the classic leak.
