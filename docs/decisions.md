# Architecture Decision Records (ADRs)

An **ADR** records a decision, the reason, and what was given up. Teams keep
these so that six months later nobody has to guess why the thing was built
this way. Add one every time you make a real choice.

Template: Context → Decision → Consequences (including the bad ones).

---

## ADR-001: Retail sales orders as the domain
**Context.** The domain wasn't specified up front, and had to be picked.
**Decision.** Retail sales orders.
**Consequences.** Familiar KPIs (revenue, category mix, top customers),
obvious quality rules (positive quantity, non-negative price, no future
dates). Gives up the chance to demonstrate event-stream handling.

---

## ADR-002: One Postgres container, three logical databases
**Context.** Airflow needs its own metadata database. Metabase needs its own
application database. Neither belongs in the analytics warehouse.
**Decision.** One container, three databases: `analytics`, `airflow`, `metabase`.
**Consequences.** Small local footprint and one thing to start. But a single
point of failure, and they compete for the same resources. In production
these would be separate instances, with the warehouse on different hardware
from Airflow's metadata.

---

## ADR-003: Incremental, file-at-a-time, made safe by idempotency
**Context.** Full refresh is simpler; incremental is what production does.
**Decision.** Process one file per task instance. Upsert on the `order_id`
primary key. Archive the file only after the load commits.
**Consequences.** Re-running is always safe, which makes recovery routine
rather than frightening. Cost: every write is an upsert, which is slower than
a bulk COPY, and `order_id` must genuinely be unique across all files
forever. If that assumption ever breaks, this design breaks with it.

---

## ADR-004: "Deploy to a test environment" means clean-slate bring-up
**Context.** The brief requires CD to a test environment. There is no cloud
budget and no target host.
**Decision.** Deployment = bring the full stack up from scratch on a clean
runner, run the smoke test, publish images to ghcr.io tagged with the commit
SHA, behind a manual approval gate.
**Consequences.** Honest and demonstrable. What is NOT demonstrated, and
would be needed for real: a persistent target host or Kubernetes cluster,
secrets from a vault rather than repository secrets, database migrations
applied as part of deploy, a rollback path for the data as well as the code,
and blue-green or canary release for warehouse tables.

---

## ADR-005: Continuous Delivery, not Continuous Deployment
**Context.** Every check passes. Should it go straight to production?
**Decision.** No. A human approves the production step.
**Consequences.** Slower releases. Justified because a bad web deploy shows a
broken page you roll back in a minute, while a bad data deploy can overwrite
a warehouse table and destroy the original in the process. The asymmetry in
blast radius is the whole argument.

---

## ADR-006: End-to-end tests do not run on every pull request
**Context.** The e2e job takes 15 minutes and occasionally flakes.
**Decision.** Run it on merge to `master`, on version tags, and nightly.
**Consequences.** A PR can be merged without proof of full-stack health,
covered by the fact that merge to master runs it before anything is deployed.
Accepted because a check people learn to ignore is worse than no check, and
15 minutes on every push is how teams start ignoring CI.

---

## ADR-007: Integration tests use service containers, not the full stack
**Context.** The integration job needs a real Postgres and MinIO.
**Decision.** GitHub Actions service containers for those two only. No
Airflow, no Metabase.
**Consequences.** 4 minutes instead of 15. Does not prove Airflow wiring,
which is exactly what the e2e job is for. Tiering checks by cost is the
central CI design skill.

---

## ADR-008: `deduplicate(keep='last')` and how it treats nulls
**Context.** The same `order_id` can appear twice in one file, sometimes with
different values (a corrected re-send later in the same export).
**Decision.** Keep the LAST occurrence. This assumes later-in-file means more
recently corrected — an assumption about the source system's export order,
not a universal truth. A null `order_id` is not "the same missing order
repeated": each null-key row is left alone here and caught separately by
validation's `order_id_not_null` check, so two unrelated null rows never
collapse into one.
**Consequences.** Correct for a source that appends corrections; wrong for a
source that prepends them. If that source ever changes its export order,
this line is the first place to look.

---

## ADR-009: which checks are fatal, which are quarantine
**Context.** Every check in `config/settings.yaml` had to be sorted into
"stop the whole file" or "drop this one row". Getting it wrong either way is
expensive: too fatal and a typo kills the nightly load; too lenient and half
a dataset ships silently.
**Decision.**
- **Fatal:** a required column missing entirely (`check_required_columns`).
  There is no per-row fallback for a column that was never there.
- **Quarantine:** `order_id_not_null`, `order_id_unique`, `customer_id_not_null`,
  `quantity_positive`, `unit_price_non_negative`, `order_date_not_future`,
  `product_category_known`. Each is a property of one row, and the file as a
  whole is still usable without that row.
- `customer_id_not_null` was added to `settings.yaml` during implementation:
  the generator's `messy` profile injects null `customer_id`, and the table
  has `customer_id NOT NULL` — without this check that row would pass
  validation and then crash the whole batch's transaction at insert time
  instead of being quarantined on its own.
**Consequences.** A missing/unparseable `quantity` or `unit_price` is treated
as failing "positive"/"non-negative" (quarantined), not silently passed
through — the alternative is a NOT NULL constraint violation aborting the
entire file's transaction over one bad row.

---

## ADR-010: revenue rounds per row, not on the sum
**Context.** `revenue = quantity * unit_price` can be rounded before or after
summing across rows; the two give different totals.
**Decision.** Round per row, at calculation time.
**Consequences.** One order's revenue never depends on which other orders
happen to be in the same batch — a property a per-order monetary value
should always have. The trade-off: summed reports carry the accumulated
rounding of many two-decimal values, rather than one rounding at the end.
Analytics signed off on per-row as the correct behaviour for this reason.

---

## ADR-011: unknown `product_category` is quarantined, not bucketed to 'other'
**Context.** A category outside the known list could be silently mapped to
an `'other'` bucket, or rejected for a human to look at.
**Decision.** Quarantine it.
**Consequences.** A brand-new category (a real product launch) shows up as a
visible rejected-rows spike instead of quietly blending into 'other' and
skewing the category breakdown. Cost: a legitimate new category needs a
person to add it to `known_categories` before its rows start landing — by
design, not by accident.

---

## ADR-012: how big a file can `download_to_bytes` hold in memory
**Context.** MinIO's `download_to_bytes` reads a whole object into memory.
**Decision.** Fine at this platform's targeted scale (5,000-50,000 rows, low
tens of MB per file).
**Consequences.** Past roughly a few hundred MB per file, this stops being
safe — a handful of concurrent tasks could exhaust a worker's memory. The
fix at that point is not a bigger worker: it's streaming the object body in
chunks (the same generator-based approach `iter_files` already uses for
listing), so memory stays flat regardless of file size.

---

## ADR-013: the read-only Postgres password is duplicated in `init.sql`
**Context.** `init.sql` runs before any environment-variable substitution is
possible, so `metabase_ro`'s password is a literal string that must match
`METABASE_READONLY_PASSWORD` in `.env` by convention, not by reference.
**Decision.** Accept the duplication for local development; document it
rather than hide it.
**Consequences.** Anyone who changes one without the other silently breaks
Metabase's connection with no error until someone opens the dashboard. A
real deployment would render this file from a template at container start
(or use a secrets manager), substituting the real value in — worth doing the
moment this stops being a local-only convenience.

---

## ADR-014: `previous_run_row_count()` reads the warehouse total, not a specific past run
**Context.** The idempotency integration test needs a stable count to compare
before and after re-loading the same file; the DAG's volume-drop guardrail
needs "how many rows did a normal run load".
**Decision.** Split into two methods instead of overloading one:
`previous_run_row_count()` returns `SELECT COUNT(*) FROM marts.sales_orders`
(the warehouse's current total — meaningful for idempotency because upserts
never grow it on a re-run), while `last_run_rows_loaded()` reads
`rows_loaded` off the most recent successful row in `staging.pipeline_runs`
(the actual per-run volume the guardrail needs). The DAG calls the second
one, captured before it writes anything this run.
**Consequences.** Two small methods instead of one over-loaded one, each
doing exactly what its caller needs — the alternative (one method serving
both) would have made the idempotency test pass trivially without proving
anything, or made the volume-drop check compare against the wrong number.
