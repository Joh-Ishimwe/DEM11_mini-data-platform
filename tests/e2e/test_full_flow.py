"""End-to-end data flow validation.

Proves data actually moves across all four hops:
    MinIO (Ingestion) -> Airflow (Processing) -> PostgreSQL (Storage)
    -> Metabase (API check)

Needs the FULL stack running. Slow, and occasionally flaky. Run it on merge
to main and nightly, NOT on every commit to every branch: fast checks on
every push, expensive checks less often, is the core cost/coverage trade-off
of CI design.
"""

from __future__ import annotations

import os
import time
import uuid
from dataclasses import replace
from datetime import datetime, timezone

import pytest
import requests

pytestmark = pytest.mark.e2e

AIRFLOW_URL = os.environ.get("AIRFLOW_URL", "http://localhost:8080")
METABASE_URL = os.environ.get("METABASE_URL", "http://localhost:3000")
AIRFLOW_USER = os.environ.get("AIRFLOW_ADMIN_USER", "admin")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_ADMIN_PASSWORD", "change_me_locally")
METABASE_ADMIN_EMAIL = os.environ.get("METABASE_ADMIN_EMAIL", "admin@example.com")
METABASE_ADMIN_PASSWORD = os.environ.get(
    "METABASE_ADMIN_PASSWORD",
    os.environ.get("AIRFLOW_ADMIN_PASSWORD", "change_me_locally"),
)

DAG_ID = "sales_ingestion"
SEED = 1234
ROWS = 200


def wait_for(predicate, timeout: int = 300, interval: int = 5, description: str = ""):
    """Poll until predicate() is truthy, or raise on timeout.

    NEVER write `time.sleep(60)` in a test. A fixed sleep is either too short
    (flaky) or too long (slow), and usually manages to be both on different
    machines. Poll with a deadline instead.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    raise TimeoutError(f"timed out after {timeout}s waiting for {description}")


@pytest.fixture
def airflow_session() -> requests.Session:
    session = requests.Session()
    session.auth = (AIRFLOW_USER, AIRFLOW_PASSWORD)
    return session


def _run_dag(session: requests.Session, run_id: str) -> str:
    """Trigger a DAG run and poll it to a terminal state. Returns that state."""
    session.patch(
        f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}", json={"is_paused": False}
    ).raise_for_status()
    session.post(
        f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}/dagRuns", json={"dag_run_id": run_id}
    ).raise_for_status()

    def _get_state() -> str:
        resp = session.get(f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}/dagRuns/{run_id}")
        resp.raise_for_status()
        return resp.json()["state"]

    wait_for(
        lambda: _get_state() in ("success", "failed"),
        timeout=600,
        interval=10,
        description=run_id,
    )
    return _get_state()


def test_data_flows_all_four_hops(pg_config, minio_config, airflow_session):
    """MinIO -> Airflow -> Postgres -> Metabase, with idempotency proven at the end.

    The expected valid/rejected counts are computed by running the SAME
    cleaning/validation code the pipeline itself uses against the file we
    are about to upload, rather than a hand-typed number that would silently
    drift out of sync the moment either the generator or the rules change.
    """
    from sqlalchemy import create_engine, text

    from data_generator.generate import build
    from src.ingestion.minio_client import MinioClient
    from src.transformations import cleaning, validation
    from src.utils.constants import load_settings

    # 1. Build a known file and work out what a correct run should produce.
    df = build(rows=ROWS, profile="messy", seed=SEED)
    source_file = f"sales/e2e_{uuid.uuid4().hex[:8]}.csv"
    payload = df.to_csv(index=False).encode("utf-8")

    cleaned = cleaning.clean(
        df, source_file=source_file, ingested_at=datetime.now(timezone.utc)
    )
    settings = load_settings()
    result = validation.validate(cleaned, settings)
    expected_valid_rows = len(result.valid)
    expected_rejected_rows = len(result.rejected)

    # 2. Upload it to the raw bucket.
    minio = MinioClient(minio_config)
    minio.upload_bytes(minio_config.raw_bucket, source_file, payload)

    # 3 + 4. Trigger the DAG and wait for it to finish.
    state = _run_dag(airflow_session, run_id=f"e2e-{uuid.uuid4().hex[:8]}")
    assert state == "success", f"DAG run ended in state={state}"

    # 5 + 6. Postgres got exactly what validation said it should.
    engine = create_engine(pg_config.sqlalchemy_url)
    with engine.connect() as conn:
        loaded = conn.execute(
            text("SELECT COUNT(*) FROM marts.sales_orders WHERE source_file = :f"),
            {"f": source_file},
        ).scalar()
        rejected = conn.execute(
            text("SELECT COUNT(*) FROM staging.rejected_rows WHERE source_file = :f"),
            {"f": source_file},
        ).scalar()
    assert loaded == expected_valid_rows
    assert rejected == expected_rejected_rows

    # 7. The object moved from raw/ to archive/.
    assert source_file not in minio.list_new_files(prefix=source_file)
    archive_client = MinioClient(
        replace(minio_config, raw_bucket=minio_config.archive_bucket)
    )
    filename = source_file.rsplit("/", 1)[-1]
    archived = archive_client.list_new_files(prefix="sales/")
    assert any(
        key.endswith(filename) for key in archived
    ), "file never reached archive/"

    # 8. Metabase is alive.
    health = requests.get(f"{METABASE_URL}/api/health", timeout=10)
    assert health.status_code == 200

    # 9. Re-run the SAME file. The row count must NOT change - idempotency,
    # proven in CI, automatically, forever. This is the single most valuable
    # assertion in this file.
    minio.upload_bytes(minio_config.raw_bucket, source_file, payload)
    second_state = _run_dag(airflow_session, run_id=f"e2e-{uuid.uuid4().hex[:8]}")
    assert second_state == "success"

    with engine.connect() as conn:
        loaded_again = conn.execute(
            text("SELECT COUNT(*) FROM marts.sales_orders WHERE source_file = :f"),
            {"f": source_file},
        ).scalar()
    assert loaded_again == expected_valid_rows


def test_metabase_can_reach_the_analytics_database():
    """Proves the 4th hop: Metabase (API check).

    Requires scripts/setup_metabase.py to have already connected the
    'analytics' database (the CI job runs it before this test; locally, run
    `make metabase-setup` after `make up`).
    """
    session = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": METABASE_ADMIN_EMAIL, "password": METABASE_ADMIN_PASSWORD},
        timeout=10,
    )
    session.raise_for_status()
    headers = {"X-Metabase-Session": session.json()["id"]}

    databases = requests.get(
        f"{METABASE_URL}/api/database", headers=headers, timeout=10
    ).json()["data"]
    analytics = next((d for d in databases if d["name"] == "analytics"), None)
    assert (
        analytics is not None
    ), "Metabase has no 'analytics' connection - run scripts/setup_metabase.py"

    query = requests.post(
        f"{METABASE_URL}/api/dataset",
        headers=headers,
        json={
            "type": "native",
            "native": {"query": "SELECT 1"},
            "database": analytics["id"],
        },
        timeout=15,
    )
    query.raise_for_status()
    assert query.json()["status"] == "completed"
