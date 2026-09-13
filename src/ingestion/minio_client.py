"""MinIO / S3 access.

Every method here follows the same rules: a timeout on every network call,
TransientError for things worth retrying, PermanentError for things that
aren't, and structured logs instead of print().
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterator

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError, ReadTimeoutError

from src.monitoring.logging import get_logger
from src.utils.constants import MinioConfig
from src.utils.errors import PermanentError, TransientError
from src.utils.retry import retry_on_transient

log = get_logger(__name__)

# HTTP status codes that mean "the server is having a bad time, try again".
_TRANSIENT_STATUS = {500, 502, 503, 504, 408, 429}


def _translate(exc: Exception) -> Exception:
    """Map a boto3 exception onto our Transient/Permanent split.

    This function is the whole reason the error hierarchy exists. Without it,
    every caller would be re-deciding what is retryable, inconsistently.
    """
    if isinstance(exc, (EndpointConnectionError, ReadTimeoutError, ConnectionError)):
        return TransientError(f"connection problem reaching object storage: {exc}")
    if isinstance(exc, ClientError):
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        code = exc.response.get("Error", {}).get("Code")
        if status in _TRANSIENT_STATUS:
            return TransientError(f"object storage returned {status}: {code}")
        # 404 NoSuchKey, 403 AccessDenied, NoSuchBucket: retrying changes nothing.
        return PermanentError(f"object storage rejected the request ({status} {code})")
    return exc


class MinioClient:
    def __init__(self, config: MinioConfig) -> None:
        self.config = config
        self._client = boto3.client(
            "s3",
            endpoint_url=config.endpoint,
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
            config=Config(
                # Timeouts on EVERY call. A hung socket with no timeout will
                # block a pipeline task forever and Airflow will show it as
                # 'running' all night. This is not optional.
                connect_timeout=config.timeout_seconds,
                read_timeout=config.timeout_seconds,
                # boto3's own retries are disabled: we handle retries in one
                # place (retry_on_transient) so backoff is consistent and
                # visible in the logs.
                retries={"max_attempts": 0},
            ),
        )

    @retry_on_transient(max_attempts=3)
    def list_new_files(self, prefix: str = "") -> list[str]:
        """Return object keys in the raw bucket under `prefix`.

        Uses a PAGINATOR, not a single list_objects_v2 call. A plain call
        returns at most 1000 keys and silently truncates. That truncation is
        a classic production bug: it works for months, then the bucket grows.
        """
        try:
            paginator = self._client.get_paginator("list_objects_v2")
            keys: list[str] = []
            for page in paginator.paginate(
                Bucket=self.config.raw_bucket, Prefix=prefix
            ):
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"])
            return keys
        except Exception as exc:  # noqa: BLE001 - translated below
            raise _translate(exc) from exc

    @retry_on_transient(max_attempts=3)
    def download_to_bytes(self, key: str) -> bytes:
        """Download one object into memory.

        Fine at this platform's scale (a few tens of MB per file). Past a few
        hundred MB, this should stream the body in chunks instead - see
        ADR-012 in docs/decisions.md.
        """
        try:
            response = self._client.get_object(Bucket=self.config.raw_bucket, Key=key)
            return response["Body"].read()
        except Exception as exc:  # noqa: BLE001
            raise _translate(exc) from exc

    @retry_on_transient(max_attempts=3)
    def upload_bytes(self, bucket: str, key: str, payload: bytes) -> None:
        """Upload bytes to a bucket. Used by the data generator and by tests."""
        try:
            self._client.put_object(Bucket=bucket, Key=key, Body=payload)
        except Exception as exc:  # noqa: BLE001
            raise _translate(exc) from exc

    def archive(self, key: str, archive_prefix: str) -> str:
        """Move a processed object from raw/ to archive/<date>/.

        S3 has no atomic move, so this is copy-then-delete. If the delete
        fails, the file just sits in raw/ and gets reprocessed tomorrow -
        harmless, since upsert_orders is idempotent. Call this only after the
        database load has committed, for the same reason.
        """
        try:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            filename = key.rsplit("/", 1)[-1]
            archive_key = "/".join(
                p for p in (archive_prefix.strip("/"), today, filename) if p
            )
            self._client.copy_object(
                Bucket=self.config.archive_bucket,
                CopySource={"Bucket": self.config.raw_bucket, "Key": key},
                Key=archive_key,
            )
            self._client.delete_object(Bucket=self.config.raw_bucket, Key=key)
            return archive_key
        except Exception as exc:  # noqa: BLE001
            raise _translate(exc) from exc

    def iter_files(self, prefix: str = "") -> Iterator[tuple[str, bytes]]:
        """Yield (key, content) pairs one at a time.

        A generator, not a list, so memory stays flat regardless of how many
        files are waiting.
        """
        for key in self.list_new_files(prefix=prefix):
            yield key, self.download_to_bytes(key)
