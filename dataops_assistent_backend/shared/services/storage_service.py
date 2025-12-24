import os
import time
import re
import json
import asyncio
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime

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
        self.logger = logging.getLogger("dataops")

        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
            config=BotoConfig(s3={"addressing_style": "path" if self.use_path_style else "auto"}),
        )

        # Ensure main bucket exists
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError:
            # MinIO accepts simple create; for real S3 in some regions you may need LocationConstraint
            try:
                self.client.create_bucket(Bucket=self.bucket)
            except ClientError as e:
                raise RuntimeError(f"Failed to ensure bucket '{self.bucket}': {e}")

        # Initialize pipeline buckets
        self.pipeline_buckets = {
            "pipelines": "pipelines",           # Main pipeline storage
            "pipeline-code": "pipeline-code",   # Generated Python code
            "pipeline-specs": "pipeline-specs", # Pipeline specifications
            "pipeline-tests": "pipeline-tests", # Test files and results
            "pipeline-logs": "pipeline-logs"    # Execution logs
        }
        
        # Initialize buckets asynchronously (will be called from startup)
        # asyncio.create_task(self._initialize_pipeline_buckets())

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

    # Pipeline Management Methods
    async def initialize_pipeline_buckets(self):
        """Initialize required buckets for pipeline storage"""
        self.logger.info("Initializing MinIO pipeline buckets...")
        
        await asyncio.to_thread(self._create_buckets_sync, list(self.pipeline_buckets.values()))
        
    def _create_buckets_sync(self, buckets: List[str]):
        """Create buckets synchronously (S3/MinIO client is sync)"""
        for bucket in buckets:
            try:
                self.client.head_bucket(Bucket=bucket)
                self.logger.debug(f"Bucket '{bucket}' already exists")
            except ClientError:
                try:
                    self.client.create_bucket(Bucket=bucket)
                    self.logger.info(f"Created MinIO bucket: {bucket}")
                except ClientError as e:
                    self.logger.error(f"Error creating bucket {bucket}: {e}")

    async def store_pipeline(self, pipeline_id: str, pipeline_data: Dict[str, Any]) -> Dict[str, str]:
        """Store complete pipeline with versioning"""
        stored_files = {}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            self.logger.info(f"Storing pipeline {pipeline_id} with version {timestamp}")
            
            pipeline_path = f"{pipeline_id}/v{timestamp}"
            # Store pipeline code
            if 'pipeline' in pipeline_data:
                code_path = f"{pipeline_path}/pipeline.py"
                await self._store_text_file("pipeline-code", code_path, pipeline_data['pipeline'])
                stored_files['pipeline'] = f"s3://pipeline-code/{code_path}"
            
            # Store requirements.txt
            if 'requirements' in pipeline_data:
                req_path = f"{pipeline_path}/requirements.txt"
                await self._store_text_file("pipeline-code", req_path, pipeline_data['requirements'])
                stored_files['requirements'] = f"s3://pipeline-code/{req_path}"

            if 'test_code' in pipeline_data:
                test_path = f"{pipeline_path}/test.py"
                await self._store_text_file("pipeline-tests", test_path, pipeline_data['test_code'])
                stored_files['test_code'] = f"s3://pipeline-tests/{test_path}"

            if 'env_template' in pipeline_data:
                env_path = f"{pipeline_path}/.env"
                await self._store_text_file("pipeline-code", env_path, pipeline_data['env_template'])
                stored_files['.env'] = f"s3://pipeline-code/{env_path}"

            if 'dockerfile' in pipeline_data:
                dockerfile_path = f"{pipeline_path}/Dockerfile"
                await self._store_text_file("pipeline-code", dockerfile_path, pipeline_data['dockerfile'])
                stored_files['dockerfile'] = f"s3://pipeline-code/{dockerfile_path}"
            
            # Store pipeline specification
            if 'spec' in pipeline_data:
                spec_path = f"{pipeline_id}/v{timestamp}/spec.json"
                await self._store_json_file("pipeline-specs", spec_path, pipeline_data['spec'])
                stored_files['spec'] = f"s3://pipeline-specs/{spec_path}"
            
            # Store test code
            if 'test_code' in pipeline_data:
                test_path = f"{pipeline_id}/v{timestamp}/test.py"
                await self._store_text_file("pipeline-tests", test_path, pipeline_data['test_code'])
                stored_files['test_code'] = f"s3://pipeline-tests/{test_path}"
            
            # Store test results
            if 'test_results' in pipeline_data:
                results_path = f"{pipeline_id}/v{timestamp}/test_results.json"
                await self._store_json_file("pipeline-tests", results_path, pipeline_data['test_results'])
                stored_files['test_results'] = f"s3://pipeline-tests/{results_path}"
            
            # Store execution logs
            if 'logs' in pipeline_data:
                logs_path = f"{pipeline_id}/v{timestamp}/execution.log"
                await self._store_text_file("pipeline-logs", logs_path, pipeline_data['logs'])
                stored_files['logs'] = f"s3://pipeline-logs/{logs_path}"
            
            # Store metadata
            metadata = {
                "pipeline_id": pipeline_id,
                "version": timestamp,
                "created_at": datetime.now().isoformat(),
                "stored_files": stored_files,
                "file_count": len(stored_files)
            }
            metadata_path = f"{pipeline_id}/v{timestamp}/metadata.json"
            await self._store_json_file("pipelines", metadata_path, metadata)
            stored_files['metadata'] = f"s3://pipelines/{metadata_path}"
            
            self.logger.info(f"Stored {len(stored_files)} files for pipeline {pipeline_id}")
            return stored_files
            
        except Exception as e:
            self.logger.error(f"Error storing pipeline: {e}")
            raise

    async def retrieve_pipeline(self, pipeline_id: str, version: Optional[str] = None) -> Dict[str, Any]:
        """Retrieve pipeline by ID and version"""
        try:
            # If no version specified, get the latest
            if not version:
                version = await self._get_latest_version(pipeline_id)
            
            if not version:
                raise ValueError(f"No pipeline found for ID {pipeline_id}")
            
            # Get metadata first
            metadata_path = f"{pipeline_id}/v{version}/metadata.json"
            metadata = await self._retrieve_json_file("pipelines", metadata_path)
            
            pipeline_data = {"metadata": metadata}
            
            # Retrieve all stored files
            for file_type, s3_path in metadata["stored_files"].items():
                if file_type != "metadata":
                    bucket, path = self._parse_s3_path(s3_path)
                    
                    if file_type in ["spec", "test_results"]:
                        pipeline_data[file_type] = await self._retrieve_json_file(bucket, path)
                    else:
                        pipeline_data[file_type] = await self._retrieve_text_file(bucket, path)
            
            return pipeline_data
            
        except Exception as e:
            self.logger.error(f"Error retrieving pipeline: {e}")
            raise

    async def list_pipeline_versions(self, pipeline_id: str) -> List[Dict[str, Any]]:
        """List all versions of a pipeline"""
        try:
            prefix = f"{pipeline_id}/"
            
            # List objects in pipelines bucket
            response = await asyncio.to_thread(
                self.client.list_objects_v2,
                Bucket="pipelines",
                Prefix=prefix
            )
            
            versions = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith("/metadata.json"):
                        # Extract version from path: pipeline_id/vYYYYMMDD_HHMMSS/metadata.json
                        path_parts = obj['Key'].split("/")
                        if len(path_parts) >= 3 and path_parts[1].startswith("v"):
                            version = path_parts[1][1:]  # Remove 'v' prefix
                            versions.append({
                                "version": version,
                                "created_at": obj['LastModified'].isoformat(),
                                "size": obj['Size'],
                                "path": obj['Key']
                            })
            
            # Sort by version (timestamp) descending
            versions.sort(key=lambda x: x["version"], reverse=True)
            return versions
            
        except Exception as e:
            self.logger.error(f"Error listing pipeline versions: {e}")
            return []

    async def delete_pipeline(self, pipeline_id: str, version: Optional[str] = None):
        """Delete pipeline versions"""
        try:
            if version:
                # Delete specific version
                prefix = f"{pipeline_id}/v{version}/"
                self.logger.info(f"Deleting pipeline {pipeline_id} version {version}")
            else:
                # Delete all versions
                prefix = f"{pipeline_id}/"
                self.logger.info(f"Deleting all versions of pipeline {pipeline_id}")
            
            # Delete from all pipeline buckets
            for bucket in self.pipeline_buckets.values():
                await self._delete_objects_with_prefix(bucket, prefix)
            
            self.logger.info(f"Deleted pipeline {pipeline_id}" + (f" version {version}" if version else " (all versions)"))
            
        except Exception as e:
            self.logger.error(f"Error deleting pipeline: {e}")
            raise

    async def _store_text_file(self, bucket: str, path: str, content: str):
        """Store text content as file"""
        content_bytes = content.encode('utf-8')
        
        await asyncio.to_thread(
            self.client.put_object,
            Bucket=bucket,
            Key=path,
            Body=content_bytes,
            ContentType='text/plain'
        )

    async def _store_json_file(self, bucket: str, path: str, data: Dict[str, Any]):
        """Store JSON data as file"""
        json_content = json.dumps(data, indent=2, default=str)
        await self._store_text_file(bucket, path, json_content)

    async def _retrieve_text_file(self, bucket: str, path: str) -> str:
        """Retrieve text file content"""
        response = await asyncio.to_thread(
            self.client.get_object,
            Bucket=bucket,
            Key=path
        )
        content = response['Body'].read().decode('utf-8')
        return content

    async def _retrieve_json_file(self, bucket: str, path: str) -> Dict[str, Any]:
        """Retrieve JSON file content"""
        content = await self._retrieve_text_file(bucket, path)
        return json.loads(content)

    async def _get_latest_version(self, pipeline_id: str) -> Optional[str]:
        """Get the latest version of a pipeline"""
        versions = await self.list_pipeline_versions(pipeline_id)
        return versions[0]["version"] if versions else None

    async def _delete_objects_with_prefix(self, bucket: str, prefix: str):
        """Delete all objects with given prefix in bucket"""
        try:
            response = await asyncio.to_thread(
                self.client.list_objects_v2,
                Bucket=bucket,
                Prefix=prefix
            )
            
            if 'Contents' in response:
                # Delete objects in batches
                objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                
                if objects_to_delete:
                    await asyncio.to_thread(
                        self.client.delete_objects,
                        Bucket=bucket,
                        Delete={'Objects': objects_to_delete}
                    )
                    self.logger.debug(f"Deleted {len(objects_to_delete)} objects from {bucket}/{prefix}")
                    
        except ClientError as e:
            # Ignore if bucket doesn't exist or is empty
            if e.response['Error']['Code'] not in ['NoSuchBucket', 'NoSuchKey']:
                raise

    def _parse_s3_path(self, s3_path: str) -> tuple:
        """Parse S3 path into bucket and object path"""
        # s3://bucket-name/path/to/object
        parts = s3_path.replace("s3://", "").split("/", 1)
        return parts[0], parts[1] if len(parts) > 1 else ""

    def generate_presigned_url(self, bucket: str, object_name: str, expires: int = 3600) -> str:
        """Generate presigned URL for file download"""
        try:
            url = self.client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket, 'Key': object_name},
                ExpiresIn=expires
            )
            return url
        except ClientError as e:
            self.logger.error(f"Error generating presigned URL: {e}")
            raise

    async def get_storage_status(self) -> Dict[str, Any]:
        """Get MinIO storage status and bucket information"""
        try:
            bucket_status = {}
            
            for bucket_name, bucket_key in self.pipeline_buckets.items():
                try:
                    # Check if bucket exists
                    await asyncio.to_thread(self.client.head_bucket, Bucket=bucket_key)
                    
                    # Count objects in bucket
                    response = await asyncio.to_thread(
                        self.client.list_objects_v2,
                        Bucket=bucket_key
                    )
                    
                    object_count = response.get('KeyCount', 0)
                    total_size = sum(obj.get('Size', 0) for obj in response.get('Contents', []))
                    
                    bucket_status[bucket_name] = {
                        "exists": True,
                        "object_count": object_count,
                        "total_size": total_size
                    }
                except ClientError as e:
                    bucket_status[bucket_name] = {
                        "exists": False,
                        "error": str(e)
                    }
            
            return {
                "status": "connected",
                "endpoint": self.endpoint,
                "buckets": bucket_status,
                "total_buckets": len([b for b in bucket_status.values() if b.get("exists")])
            }
            
        except Exception as e:
            self.logger.error(f"Error getting storage status: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
