"""Run the pipeline WITHOUT Airflow, for fast local iteration.

Why this exists: waiting for the scheduler to pick up a DAG, then reading the
UI, is a slow feedback loop when you are debugging a transformation. This
script calls the same functions from src/ directly, so you get a traceback in
your terminal in two seconds.

It is a development tool. It is NOT the pipeline. The DAG is the pipeline.

    python scripts/run_local_pipeline.py --file data/samples/sales_messy_42_5000.csv
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.monitoring.logging import get_logger, set_run_id  # noqa: E402
from src.utils.constants import load_settings  # noqa: E402

log = get_logger("local_runner")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=Path, required=True)
    parser.add_argument(
        "--no-load", action="store_true", help="transform only, skip the database"
    )
    args = parser.parse_args()

    run_id = f"local:{uuid.uuid4().hex[:8]}"
    set_run_id(run_id)
    settings = load_settings()
    now = datetime.now(timezone.utc)

    log.info("run_started", extra={"source_file": str(args.file)})
    df = pd.read_csv(args.file)
    log.info("file_read", extra={"rows": len(df)})

    from src.transformations import business_rules, cleaning, validation
    from src.utils.constants import REQUIRED_COLUMNS

    df = cleaning.clean(df, source_file=args.file.name, ingested_at=now)
    validation.check_required_columns(df, REQUIRED_COLUMNS)
    result = validation.validate(df, settings)
    valid = business_rules.apply_business_rules(result.valid)
    log.info(
        "validated",
        extra={
            "valid": len(valid),
            "rejected": len(result.rejected),
            "reasons": result.reasons,
        },
    )

    if not args.no_load:
        from src.storage.postgres import PostgresStorage
        from src.utils.constants import PostgresConfig

        storage = PostgresStorage(PostgresConfig.from_env())
        storage.start_run(
            run_id=run_id, dag_id="local_runner", source_file=args.file.name
        )
        loaded = storage.upsert_orders(valid, run_id=run_id)
        rejected = storage.quarantine(
            result.rejected, run_id=run_id, source_file=args.file.name
        )
        storage.finish_run(
            run_id,
            status="success",
            rows_read=len(df),
            rows_loaded=loaded,
            rows_rejected=rejected,
        )
        log.info("loaded", extra={"rows_loaded": loaded, "rows_rejected": rejected})

    log.info("run_finished")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
