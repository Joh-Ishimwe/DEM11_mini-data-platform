"""Central configuration loader.

Why this file exists: config should be read in ONE place and passed around,
not read via os.getenv() scattered through twenty modules. When a variable
is renamed you want to change one line, not grep the whole repo.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SETTINGS_PATH = PROJECT_ROOT / "config" / "settings.yaml"

# Columns the pipeline expects from the source. Missing any of these is FATAL:
# there is no sensible way to continue without them.
REQUIRED_COLUMNS: tuple[str, ...] = (
    "order_id",
    "customer_id",
    "order_date",
    "product_category",
    "quantity",
    "unit_price",
    "currency",
    "country",
)

# Columns the pipeline itself adds. Never expected from the source.
LINEAGE_COLUMNS: tuple[str, ...] = (
    "revenue",
    "order_size_bucket",
    "source_file",
    "ingested_at",
)


def load_settings(path: Path | None = None) -> dict[str, Any]:
    """Read config/settings.yaml into a plain dict."""
    with open(path or SETTINGS_PATH) as fh:
        return yaml.safe_load(fh)


@dataclass(frozen=True)
class PostgresConfig:
    """Connection details for the analytics warehouse.

    frozen=True makes it immutable: nothing downstream can accidentally
    mutate the config halfway through a run.
    """

    user: str
    password: str
    host: str
    port: int
    database: str
    statement_timeout_seconds: int = 60

    @classmethod
    def from_env(cls) -> "PostgresConfig":
        return cls(
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            host=os.environ.get("POSTGRES_HOST", "postgres"),
            port=int(os.environ.get("POSTGRES_PORT", 5432)),
            database=os.environ.get("ANALYTICS_DB", "analytics"),
        )

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    def __repr__(self) -> str:  # pragma: no cover
        # NEVER let a password reach a log line. Printing a whole config object
        # is the single most common way secrets leak into CI logs.
        return f"PostgresConfig(host={self.host}, port={self.port}, database={self.database})"


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    raw_bucket: str
    archive_bucket: str
    timeout_seconds: int = 30

    @classmethod
    def from_env(cls) -> "MinioConfig":
        return cls(
            endpoint=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            access_key=os.environ["MINIO_ROOT_USER"],
            secret_key=os.environ["MINIO_ROOT_PASSWORD"],
            raw_bucket=os.environ.get("MINIO_RAW_BUCKET", "raw"),
            archive_bucket=os.environ.get("MINIO_ARCHIVE_BUCKET", "archive"),
        )

    def __repr__(self) -> str:  # pragma: no cover
        return f"MinioConfig(endpoint={self.endpoint}, raw_bucket={self.raw_bucket})"
