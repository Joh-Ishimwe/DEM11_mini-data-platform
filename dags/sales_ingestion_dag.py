"""Airflow DAG: MinIO -> clean -> validate -> Postgres -> archive.

This file is deliberately thin: orchestration only, no business logic. Every
real piece of logic is imported from src/, because anything written directly
in a DAG file can only be tested by running Airflow, which is slow. Anything
in src/ runs in milliseconds in a normal pytest run.

Each pipeline stage (extract/transform/validate/enrich/load/quarantine/
archive) is its own task inside a mapped `process_file` task group, one group
instance per file. That is more XCom traffic than a single task doing
everything (each stage hands the next a JSON-serialised DataFrame), but it
means the Graph view shows exactly which stage a given file failed at,
instead of one opaque node.
"""

from __future__ import annotations

import io
from datetime import datetime, timedelta, timezone

import pandas as pd
from airflow.decorators import dag, task, task_group

from src.monitoring.logging import get_logger, set_run_id

log = get_logger(__name__)

DAG_ID = "sales_ingestion"

default_args = {
    "owner": "data-engineering",
    # Airflow-level retries handle infrastructure flakiness (a worker dying).
    # The retry_on_transient decorator in src/ handles call-level flakiness.
    # They are different layers and both are needed.
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    # Without this, a hung task shows as 'running' all night and nobody is paged.
    "execution_timeout": timedelta(minutes=30),
    "depends_on_past": False,
}

# Columns that must round-trip through XCom as real datetimes, not strings.
# Everything else is left alone so read_json never guesses wrong.
_DATE_COLUMNS = ["order_date", "ingested_at"]


def notify_failure(context) -> None:
    """on_failure_callback: log enough that someone can act without digging.

    Wired to Slack (or email) via SLACK_WEBHOOK_URL if it's set in the
    environment; otherwise this just logs, which is what CI and local runs do.
    Only fires on a task failure - that is "must act tonight", everything else
    stays a log line or a dashboard card.
    """
    import json
    import os
    import urllib.request

    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    log_url = context["task_instance"].log_url
    run_id = context.get("run_id", "-")

    log.error(
        "dag_task_failed",
        extra={
            "dag_id": dag_id,
            "task_id": task_id,
            "run_id": run_id,
            "log_url": log_url,
        },
    )

    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    message = {
        "text": (
            f":red_circle: *{dag_id}* failed on task `{task_id}`\n"
            f"run_id: {run_id}\n<{log_url}|Open the task log>"
        )
    }
    try:
        request = urllib.request.Request(
            webhook_url,
            data=json.dumps(message).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(
            request, timeout=10
        )  # noqa: S310 - trusted, internal webhook
    except Exception as exc:  # noqa: BLE001 - a failed alert must never fail the DAG
        log.warning("failure_notification_not_sent", extra={"error": str(exc)})


def _serialize(df: pd.DataFrame) -> str:
    """DataFrame -> JSON string, safe to pass through XCom."""
    return df.to_json(orient="records", date_format="iso")


def _deserialize(payload: str, date_columns: list[str] | None = None) -> pd.DataFrame:
    """JSON string -> DataFrame. `date_columns` opts specific columns back
    into real datetimes; everything else stays exactly as written, so this
    never silently reinterprets a column read_json's heuristics guess wrong."""
    return pd.read_json(io.StringIO(payload), orient="records", convert_dates=date_columns or False)


def _record_run_failure(context) -> None:
    """on_failure_callback for every process_file step after `begin`.

    Whichever step raises is the only one whose callback fires - downstream
    steps never run under the default trigger rule - so this writes
    finish_run(status='failed') exactly once per file, with whatever counts
    the earlier, successful steps already produced. Mirrors the old
    single-task version's except block, which had the same partial counts
    sitting in local variables when it caught an exception.
    """
    from src.storage.postgres import PostgresStorage
    from src.utils.constants import PostgresConfig

    ti = context["task_instance"]
    group = "process_file"
    began = ti.xcom_pull(task_ids=f"{group}.begin", map_indexes=ti.map_index)
    if not began:
        return  # start_run() itself never ran; no run row exists to reconcile

    extracted = (
        ti.xcom_pull(task_ids=f"{group}.extract", map_indexes=ti.map_index) or {}
    )
    loaded = (
        ti.xcom_pull(task_ids=f"{group}.load_valid", map_indexes=ti.map_index) or {}
    )
    quarantined = (
        ti.xcom_pull(task_ids=f"{group}.quarantine", map_indexes=ti.map_index) or {}
    )

    PostgresStorage(PostgresConfig.from_env()).finish_run(
        began["run_id"],
        status="failed",
        rows_read=extracted.get("rows_read", 0),
        rows_loaded=loaded.get("rows_loaded", 0),
        rows_rejected=quarantined.get("rows_rejected", 0),
        error_message=str(context["exception"])[:2000],
    )


@dag(
    dag_id=DAG_ID,
    description="Ingest sales files from MinIO into the analytics warehouse",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="0 2 * * *",  # 02:00 daily
    catchup=False,  # do NOT backfill a year on first unpause
    max_active_runs=1,  # two concurrent runs on one bucket = chaos
    default_args=default_args,
    on_failure_callback=notify_failure,
    tags=["sales", "ingestion"],
)
def sales_ingestion():

    @task
    def list_files() -> list[str]:
        """New object keys waiting in the raw bucket. An empty list is a
        SUCCESS, not a failure - a quiet weekend with no files is normal."""
        from src.ingestion.minio_client import MinioClient
        from src.utils.constants import MinioConfig

        client = MinioClient(MinioConfig.from_env())
        return client.list_new_files(prefix="sales/")

    @task
    def snapshot_baseline() -> int:
        """Rows loaded by the last successful run, captured before this run
        writes anything, so the volume-drop check has something honest to
        compare against."""
        from src.storage.postgres import PostgresStorage
        from src.utils.constants import PostgresConfig

        return PostgresStorage(PostgresConfig.from_env()).last_run_rows_loaded()

    @task_group(group_id="process_file")
    def process_file(key: str):
        """Download, clean, validate, load, archive - one file, end to end,
        as one task per stage so the Graph view shows exactly where a file
        got stuck instead of one opaque node."""

        @task
        def begin(key: str) -> dict:
            """Open the run record. If start_run() itself fails, nothing
            downstream runs and there is no run row to reconcile - same as
            the old code, where a failed start_run() call raised before the
            try/except even began."""
            from src.storage.postgres import PostgresStorage
            from src.utils.constants import PostgresConfig

            run_id = (
                f"{DAG_ID}:{key}:"
                f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}"
            )
            set_run_id(run_id)
            PostgresStorage(PostgresConfig.from_env()).start_run(
                run_id=run_id, dag_id=DAG_ID, source_file=key
            )
            return {"key": key, "run_id": run_id}

        @task(on_failure_callback=_record_run_failure)
        def extract(ctx: dict) -> dict:
            """Download the raw file from MinIO."""
            from src.ingestion.minio_client import MinioClient
            from src.utils.constants import MinioConfig

            set_run_id(ctx["run_id"])
            minio = MinioClient(MinioConfig.from_env())
            raw_bytes = minio.download_to_bytes(ctx["key"])
            raw_df = pd.read_csv(io.BytesIO(raw_bytes))
            return {**ctx, "rows_read": len(raw_df), "data": _serialize(raw_df)}

        @task(on_failure_callback=_record_run_failure)
        def transform(extracted: dict) -> dict:
            """Clean: normalise columns, coerce types, dedupe, attach lineage."""
            from src.transformations import cleaning, validation
            from src.utils.constants import REQUIRED_COLUMNS

            set_run_id(extracted["run_id"])
            raw_df = _deserialize(extracted["data"])
            cleaned = cleaning.clean(
                raw_df,
                source_file=extracted["key"],
                ingested_at=datetime.now(timezone.utc),
            )
            # FATAL: a required column missing entirely means the file is
            # unusable, not that one row is bad. Let it raise and fail the task.
            validation.check_required_columns(cleaned, REQUIRED_COLUMNS)
            return {**extracted, "data": _serialize(cleaned)}

        @task(on_failure_callback=_record_run_failure)
        def validate(transformed: dict) -> dict:
            """Split rows into valid vs quarantined, with a reason each.
            Never raises - a bad row is data, not a pipeline failure."""
            from src.transformations import validation
            from src.utils.constants import load_settings

            set_run_id(transformed["run_id"])
            cleaned = _deserialize(transformed["data"], date_columns=_DATE_COLUMNS)
            result = validation.validate(cleaned, load_settings())
            return {
                **transformed,
                "data": None,
                "valid": _serialize(result.valid),
                "rejected": _serialize(result.rejected),
            }

        @task(on_failure_callback=_record_run_failure)
        def enrich(validated: dict) -> dict:
            """Business rules (revenue, order-size bucket) on valid rows only -
            never on rows that are about to be quarantined."""
            from src.transformations import business_rules

            set_run_id(validated["run_id"])
            valid = _deserialize(validated["valid"], date_columns=_DATE_COLUMNS)
            enriched = business_rules.apply_business_rules(valid)
            return {**validated, "valid": _serialize(enriched)}

        @task(on_failure_callback=_record_run_failure)
        def load_valid(enriched: dict) -> dict:
            """Upsert valid rows into marts.sales_orders. Idempotent: safe to
            re-run, since ON CONFLICT updates instead of duplicating."""
            from src.storage.postgres import PostgresStorage
            from src.utils.constants import PostgresConfig

            set_run_id(enriched["run_id"])
            valid = _deserialize(enriched["valid"], date_columns=_DATE_COLUMNS)
            rows_loaded = PostgresStorage(PostgresConfig.from_env()).upsert_orders(
                valid, run_id=enriched["run_id"]
            )
            return {**enriched, "valid": None, "rows_loaded": rows_loaded}

        @task(on_failure_callback=_record_run_failure)
        def quarantine(loaded: dict) -> dict:
            """Write rejected rows to staging.rejected_rows with their reason."""
            from src.storage.postgres import PostgresStorage
            from src.utils.constants import PostgresConfig

            set_run_id(loaded["run_id"])
            rejected = _deserialize(loaded["rejected"], date_columns=_DATE_COLUMNS)
            rows_rejected = PostgresStorage(PostgresConfig.from_env()).quarantine(
                rejected, run_id=loaded["run_id"], source_file=loaded["key"]
            )
            return {**loaded, "rejected": None, "rows_rejected": rows_rejected}

        @task(on_failure_callback=_record_run_failure)
        def archive(quarantined: dict) -> dict:
            """Archive only after the load has committed, then close the run.

            If archiving fails here, the run is marked failed but the file
            just stays in raw/ and gets picked up again tomorrow - upsert is
            idempotent, so reprocessing it changes nothing.
            """
            from src.ingestion.minio_client import MinioClient
            from src.storage.postgres import PostgresStorage
            from src.utils.constants import MinioConfig, PostgresConfig

            set_run_id(quarantined["run_id"])
            MinioClient(MinioConfig.from_env()).archive(
                quarantined["key"], archive_prefix="sales"
            )
            result = {
                "key": quarantined["key"],
                "rows_read": quarantined["rows_read"],
                "rows_loaded": quarantined["rows_loaded"],
                "rows_rejected": quarantined["rows_rejected"],
            }
            PostgresStorage(PostgresConfig.from_env()).finish_run(
                quarantined["run_id"],
                status="success",
                **{k: result[k] for k in ("rows_read", "rows_loaded", "rows_rejected")},
            )
            log.info("file_processed", extra=result)
            return result

        began = begin(key)
        extracted = extract(began)
        transformed = transform(extracted)
        validated = validate(transformed)
        enriched = enrich(validated)
        loaded = load_valid(enriched)
        quarantined = quarantine(loaded)
        return archive(quarantined)

    @task
    def summarise(results: list[dict], baseline: int) -> None:
        """Aggregate counts across every file this run touched, log a single
        readable summary, and run the volume-drop guardrail - a check CI can
        never do, because it needs yesterday's real numbers."""
        from src.transformations.validation import check_volume_drop
        from src.utils.constants import load_settings

        total_read = sum(r["rows_read"] for r in results)
        total_loaded = sum(r["rows_loaded"] for r in results)
        total_rejected = sum(r["rows_rejected"] for r in results)

        log.info(
            "dag_run_summary",
            extra={
                "files_processed": len(results),
                "rows_read": total_read,
                "rows_loaded": total_loaded,
                "rows_rejected": total_rejected,
            },
        )

        if not results:
            return  # nothing landed this run; nothing to compare either

        threshold = load_settings()["validation"]["row_count_drop_alert_threshold"]
        if check_volume_drop(
            current_rows=total_loaded, previous_rows=baseline, threshold=threshold
        ):
            log.warning(
                "volume_drop_detected",
                extra={
                    "current_rows": total_loaded,
                    "previous_rows": baseline,
                    "threshold": threshold,
                },
            )

    # .expand() on a @task_group function is DYNAMIC TASK GROUP MAPPING:
    # Airflow creates one process_file group instance per key returned by
    # list_files(), at runtime. One bad file then fails only its own group
    # instance, not the whole run.
    summarise(process_file.expand(key=list_files()), snapshot_baseline())


dag_instance = sales_ingestion()
