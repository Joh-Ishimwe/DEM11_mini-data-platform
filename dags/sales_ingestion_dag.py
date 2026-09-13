"""Airflow DAG: MinIO -> clean -> validate -> Postgres -> archive.

This file is deliberately thin: orchestration only, no business logic. Every
real piece of logic is imported from src/, because anything written directly
in a DAG file can only be tested by running Airflow, which is slow. Anything
in src/ runs in milliseconds in a normal pytest run.
"""

from __future__ import annotations

import io
from datetime import datetime, timedelta, timezone

import pandas as pd
from airflow.decorators import dag, task

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

    @task
    def process_file(key: str) -> dict:
        """Download, clean, validate, load, archive - one file, end to end.

        If archiving fails after the load already committed, the file just
        gets picked up again tomorrow. That's fine: upsert_orders is
        idempotent, so reprocessing it changes nothing.
        """
        from src.ingestion.minio_client import MinioClient
        from src.storage.postgres import PostgresStorage
        from src.transformations import business_rules, cleaning, validation
        from src.utils.constants import (
            REQUIRED_COLUMNS,
            MinioConfig,
            PostgresConfig,
            load_settings,
        )

        run_id = (
            f"{DAG_ID}:{key}:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}"
        )
        set_run_id(run_id)

        minio = MinioClient(MinioConfig.from_env())
        storage = PostgresStorage(PostgresConfig.from_env())
        settings = load_settings()

        storage.start_run(run_id=run_id, dag_id=DAG_ID, source_file=key)
        rows_read = rows_loaded = rows_rejected = 0
        try:
            raw_bytes = minio.download_to_bytes(key)
            raw_df = pd.read_csv(io.BytesIO(raw_bytes))
            rows_read = len(raw_df)

            now = datetime.now(timezone.utc)
            cleaned = cleaning.clean(raw_df, source_file=key, ingested_at=now)

            # Fatal: a required column missing entirely means the file is
            # unusable, not that one row is bad. Let it raise and fail the task.
            validation.check_required_columns(cleaned, REQUIRED_COLUMNS)

            result = validation.validate(cleaned, settings)
            enriched = business_rules.apply_business_rules(result.valid)

            rows_loaded = storage.upsert_orders(enriched, run_id=run_id)
            rows_rejected = storage.quarantine(
                result.rejected, run_id=run_id, source_file=key
            )

            # Archive only after the load has committed.
            minio.archive(key, archive_prefix="sales")

            storage.finish_run(
                run_id,
                status="success",
                rows_read=rows_read,
                rows_loaded=rows_loaded,
                rows_rejected=rows_rejected,
            )
            log.info(
                "file_processed",
                extra={
                    "source_file": key,
                    "rows_read": rows_read,
                    "rows_loaded": rows_loaded,
                    "rows_rejected": rows_rejected,
                },
            )
            return {
                "key": key,
                "rows_read": rows_read,
                "rows_loaded": rows_loaded,
                "rows_rejected": rows_rejected,
            }
        except Exception as exc:
            storage.finish_run(
                run_id,
                status="failed",
                rows_read=rows_read,
                rows_loaded=rows_loaded,
                rows_rejected=rows_rejected,
                error_message=str(exc)[:2000],
            )
            raise

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

    # .expand() is DYNAMIC TASK MAPPING: Airflow creates one process_file task
    # instance per key returned by list_files, at runtime. One bad file then
    # fails only its own task instance, not the whole run.
    summarise(process_file.expand(key=list_files()), snapshot_baseline())


dag_instance = sales_ingestion()
