"""Create the raw/archive MinIO buckets for the CI integration-tests job.

The job runs the official quay.io/minio/minio image directly (see main.yml), not
docker-compose's minio-init container, so nothing else creates these buckets.
Bitnami's image used to do this via a MINIO_DEFAULT_BUCKETS env var, but that
image's free tags were deprecated - this replaces it, reading the same
environment variables the rest of the pipeline uses.
"""

from __future__ import annotations

import os

import boto3
from botocore.config import Config


def main() -> None:
    client = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        config=Config(signature_version="s3v4"),
    )
    existing = {b["Name"] for b in client.list_buckets()["Buckets"]}
    for bucket in (os.environ["MINIO_RAW_BUCKET"], os.environ["MINIO_ARCHIVE_BUCKET"]):
        if bucket in existing:
            print(f"bucket already exists: {bucket}")
            continue
        client.create_bucket(Bucket=bucket)
        print(f"created bucket: {bucket}")


if __name__ == "__main__":
    main()
