# Data dictionary

## marts.sales_orders

The business-ready table. The only schema Metabase can read.

| Column | Type | Null? | Source | Meaning |
|---|---|---|---|---|
| order_id | TEXT PK | no | source | Unique order identifier. Primary key, which is what makes the load idempotent. |
| customer_id | TEXT | no | source | Customer who placed the order. |
| order_date | DATE | no | source | Date the order was placed. Never in the future. |
| product_category | TEXT | no | source | One of the known categories in settings.yaml. |
| quantity | INTEGER | no | source | Units ordered. Must be > 0. |
| unit_price | NUMERIC(12,2) | no | source | Price per unit. Must be >= 0. |
| currency | CHAR(3) | no | source | ISO 4217 code, uppercased by cleaning. |
| country | TEXT | **yes** | source | Title-cased by cleaning. Nullable by design. |
| revenue | NUMERIC(14,2) | no | **derived** | quantity * unit_price. Gross, excludes tax and shipping. Decided by Analytics. |
| order_size_bucket | TEXT | yes | **derived** | small / medium / large / enterprise. Boundaries lower-inclusive. |
| source_file | TEXT | no | **pipeline** | Object key this row came from. Lineage. Makes debugging a wrong number possible. |
| ingested_at | TIMESTAMPTZ | no | **pipeline** | When the pipeline loaded it. Not when the order happened. |

**Currency warning:** revenue is stored in the order's own currency and is
NOT converted. Summing revenue across currencies gives a meaningless number.
Either always group by currency, or add a conversion step and an ADR.

## staging.rejected_rows

Quarantine. One bad record must never kill a run, so bad records land here
instead.

| Column | Type | Meaning |
|---|---|---|
| id | BIGSERIAL | surrogate key |
| run_id | TEXT | ties back to pipeline_runs |
| source_file | TEXT | which file it came from |
| rejection_reason | TEXT | which check failed, and why |
| raw_record | JSONB | the original record, unmodified |
| rejected_at | TIMESTAMPTZ | when |

## staging.pipeline_runs

The pipeline monitoring itself. Build a Metabase card on this and pipeline
health becomes visible to the business instead of buried in logs.

| Column | Type | Meaning |
|---|---|---|
| run_id | TEXT PK | unique per file per run |
| dag_id | TEXT | which DAG |
| source_file | TEXT | which file |
| started_at / finished_at | TIMESTAMPTZ | duration |
| status | TEXT | running / success / failed |
| rows_read / rows_loaded / rows_rejected | INTEGER | the counts that matter |
| error_message | TEXT | populated on failure |

A run left at `running` forever is worse than one that records its own
failure. Close this row in a `finally`.

## marts.v_daily_rejections / marts.v_pipeline_runs

Views, not tables — read-only windows onto `staging.*` exposed inside `marts`
so Metabase's `metabase_ro` user never needs direct access to staging. Back
the dashboard's "rejected rows per day" and "recent pipeline runs" cards.
