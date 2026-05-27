from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]


class ObjectNotFound(Exception):
    def __init__(self, s3_key: str) -> None:
        super().__init__(f"S3 object not found: {s3_key}")
        self.s3_key = s3_key


class StorageError(Exception):
    pass


@dataclass(frozen=True)
class S3Settings:
    bucket: str
    endpoint_url: str | None
    region: str
    access_key_id: str | None
    secret_access_key: str | None

    @classmethod
    def from_env(cls) -> S3Settings:
        return cls(
            bucket=os.environ.get("AU_KPIS_OBJECT_STORE__BUCKET", "au-kpis-local"),
            endpoint_url=os.environ.get("AU_KPIS_OBJECT_STORE__ENDPOINT"),
            region=os.environ.get("AU_KPIS_OBJECT_STORE__REGION", "us-east-1"),
            access_key_id=os.environ.get("AU_KPIS_OBJECT_STORE__ACCESS_KEY_ID"),
            secret_access_key=os.environ.get("AU_KPIS_OBJECT_STORE__SECRET_ACCESS_KEY"),
        )


class S3StorageClient:
    def __init__(self, settings: S3Settings | None = None) -> None:
        self.settings = settings or S3Settings.from_env()
        self._client = boto3.client(
            "s3",
            endpoint_url=self.settings.endpoint_url,
            region_name=self.settings.region,
            aws_access_key_id=self.settings.access_key_id,
            aws_secret_access_key=self.settings.secret_access_key,
        )

    def fetch_to_path(self, s3_key: str, destination: Path) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            response: dict[str, Any] = self._client.get_object(
                Bucket=self.settings.bucket,
                Key=s3_key,
            )
        except ClientError as err:
            code = str(err.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                raise ObjectNotFound(s3_key) from err
            raise StorageError(f"failed to fetch S3 object {s3_key}: {code}") from err

        body = response["Body"]
        with destination.open("wb") as handle:
            while chunk := body.read(1024 * 1024):
                handle.write(chunk)
        return destination
