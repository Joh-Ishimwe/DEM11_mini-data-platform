"""Integration tests against a real MinIO."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


def test_upload_then_download_round_trip(minio_config):
    from src.ingestion.minio_client import MinioClient

    client = MinioClient(minio_config)
    payload = b"order_id,quantity\nORD-1,2\n"
    client.upload_bytes(minio_config.raw_bucket, "sales/roundtrip.csv", payload)
    assert client.download_to_bytes("sales/roundtrip.csv") == payload


def test_listing_handles_more_than_one_page(minio_config):
    """list_objects_v2 silently truncates at 1000 keys. Prove you paginate.

    This bug works fine for months and then breaks when the bucket grows,
    which makes it exactly the kind of thing a test should pin down.
    """
    from src.ingestion.minio_client import MinioClient

    client = MinioClient(minio_config)
    prefix = "sales/_pagination_test/"
    total = 1001
    for i in range(total):
        client.upload_bytes(
            minio_config.raw_bucket, f"{prefix}{i:05d}.csv", b"order_id\n"
        )

    keys = client.list_new_files(prefix=prefix)
    assert len(keys) == total


def test_missing_key_raises_permanent_not_transient(minio_config):
    """A 404 must NOT be retried. Retrying it wastes ten minutes and hides
    the real cause."""
    from src.ingestion.minio_client import MinioClient
    from src.utils.errors import PermanentError

    with pytest.raises(PermanentError):
        MinioClient(minio_config).download_to_bytes("sales/definitely-not-here.csv")
