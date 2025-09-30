import os
import time
import re
from typing import Optional

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError


SAFE_NAME = re.compile(r"[^A-Za-z0-9._+-]")


def sanitize_filename(name: str) -> str:
    base = name.split("/")[-1]
    safe = SAFE_NAME.sub("-", base)
    return safe or f"file-{int(time.time())}"


class MinioStorage:
    """
    Small wrapper around S3-compatible storage (MinIO).
    Provides presigned PUT/GET and optional direct upload helpers.
    """

    def __init__(self):
        self.endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
        self.region = os.getenv("S3_REGION", "us-east-1")
        self.bucket = os.getenv("S3_BUCKET", "dataops-bucket")
        self.access_key = os.getenv("S3_ACCESS_KEY", "minioadmin")
        self.secret_key = os.getenv("S3_SECRET_KEY", "minioadmin")
        self.use_path_style = os.getenv("S3_USE_PATH_STYLE", "true").lower() == "true"
        self.public_base_url = os.getenv("PUBLIC_S3_BASE_URL", "http://localhost:9000")
        self.expires = int(os.getenv("PRESIGN_EXPIRES_SECONDS", "600"))

        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
            config=BotoConfig(s3={"addressing_style": "path" if self.use_path_style else "auto"}),
        )

        # Ensure bucket exists
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError:
            # MinIO accepts simple create; for real S3 in some regions you may need LocationConstraint
            try:
                self.client.create_bucket(Bucket=self.bucket)
            except ClientError as e:
                raise RuntimeError(f"Failed to ensure bucket '{self.bucket}': {e}")

    def object_key(self, filename: str, prefix: str = "uploads/") -> str:
        safe = sanitize_filename(filename)
        return f"{prefix}{int(time.time())}_{safe}"

    def presigned_put(self, filename: str, content_type: Optional[str] = None, prefix: str = "uploads/"):
        key = self.object_key(filename, prefix)
        params = {"Bucket": self.bucket, "Key": key}
        if content_type:
            params["ContentType"] = content_type

        url = self.client.generate_presigned_url(
            "put_object", Params=params, ExpiresIn=self.expires
        )

        public_url = f"{self.public_base_url}/{self.bucket}/{key}"
        return {"upload_url": url, "object_key": key, "public_url": public_url, "expires_in": self.expires}

    def presigned_get(self, key: str):
        url = self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=self.expires,
        )
        return {"download_url": url, "expires_in": self.expires}

    def direct_put_bytes(self, key: str, data: bytes, content_type: Optional[str] = None):
        extra = {}
        if content_type:
            extra["ContentType"] = content_type
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, **extra)
        return {"object_key": key, "public_url": f"{self.public_base_url}/{self.bucket}/{key}"}
