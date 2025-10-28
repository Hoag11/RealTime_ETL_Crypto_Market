"""Utility helpers for working with MinIO in batch jobs."""

from __future__ import annotations

import io
from typing import Iterable, Optional

from minio import Minio
from minio.error import S3Error


def _sanitize_endpoint(endpoint: str) -> str:
    """Remove protocol prefixes from a MinIO endpoint."""
    if endpoint.startswith("http://"):
        return endpoint[len("http://") :]
    if endpoint.startswith("https://"):
        return endpoint[len("https://") :]
    return endpoint


def get_minio_client(
    endpoint: str,
    access_key: str,
    secret_key: str,
    *,
    secure: bool = False,
) -> Minio:
    """Create a MinIO client with a sanitized endpoint."""
    return Minio(
        _sanitize_endpoint(endpoint),
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )


def ensure_bucket(client: Minio, bucket_name: str) -> None:
    """Ensure that the given bucket exists on the MinIO server."""
    if not bucket_name:
        raise ValueError("bucket_name must be provided")

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def upload_json_bytes(
    client: Minio,
    bucket_name: str,
    object_name: str,
    data: bytes | str,
) -> None:
    """Upload JSON content to MinIO as a byte stream."""
    if isinstance(data, str):
        data = data.encode("utf-8")

    buffer = io.BytesIO(data)
    client.put_object(
        bucket_name,
        object_name,
        data=buffer,
        length=len(data),
        content_type="application/json",
    )


def bucket_has_objects(
    client: Minio,
    bucket_name: str,
    prefix: Optional[str] = None,
) -> bool:
    """Return True if the bucket contains at least one object for the prefix."""
    try:
        if not bucket_name:
            return False

        if not client.bucket_exists(bucket_name):
            return False

        objects: Iterable = client.list_objects(
            bucket_name,
            prefix=prefix,
            recursive=True,
        )
        for _ in objects:
            return True
        return False
    except S3Error:
        return False
