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

        # For real AWS S3, endpoint_url should be None. For MinIO, use the endpoint.
        endpoint_url = None if not self.endpoint or self.endpoint.lower() in ["", "none"] else self.endpoint
        self.logger.info(f"Initializing MinIO Storage with endpoint: {endpoint_url}, bucket: {self.bucket}")
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
            config=BotoConfig(s3={"addressing_style": "path" if self.use_path_style else "auto"}),
        )

        # Ensure main bucket exists
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError:
            # Create bucket with proper LocationConstraint for AWS S3
            try:
                if self.region == 'us-east-1':
                    # us-east-1 doesn't use LocationConstraint
                    self.client.create_bucket(Bucket=self.bucket)
                else:
                    # Other regions require LocationConstraint
                    self.client.create_bucket(
                        Bucket=self.bucket,
                        CreateBucketConfiguration={'LocationConstraint': self.region}
                    )
            except ClientError as e:
                raise RuntimeError(f"Failed to ensure bucket '{self.bucket}': {e}")

        # Initialize pipeline prefixes (all in single bucket)
        self.pipeline_prefixes = {
            "pipelines": "pipelines",           # Main pipeline storage
            "pipeline-code": "pipeline-code",   # Generated Python code
            "pipeline-specs": "pipeline-specs", # Pipeline specifications
            "pipeline-tests": "pipeline-tests", # Test files and results
            "pipeline-logs": "pipeline-logs"    # Execution logs
        }

    # Pipeline Management Methods
    async def initialize_pipeline_buckets(self):
        """Initialize main bucket for pipeline storage (single bucket with prefixes)"""
        self.logger.info(f"Initializing storage bucket: {self.bucket}...")
        # Main bucket is already created in __init__, nothing more needed

    async def store_pipeline(self, pipeline_id: str, pipeline_data: Dict[str, Any]) -> Dict[str, str]:
        """Store complete pipeline with versioning"""
        stored_files = {}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        try:
            self.logger.info(f"Storing pipeline {pipeline_id} with version {timestamp}")

            pipeline_path = f"{pipeline_id}/v{timestamp}"
            # Store pipeline code
            if 'pipeline' in pipeline_data:
                code_path = f"pipeline-code/{pipeline_path}/pipeline.py"
                await self._store_text_file(code_path, pipeline_data['pipeline'])
                stored_files['pipeline'] = f"s3://{self.bucket}/{code_path}"

            # Store requirements.txt
            if 'requirements' in pipeline_data:
                req_path = f"pipeline-code/{pipeline_path}/requirements.txt"
                await self._store_text_file(req_path, pipeline_data['requirements'])
                stored_files['requirements'] = f"s3://{self.bucket}/{req_path}"

            if 'test_code' in pipeline_data:
                test_path = f"pipeline-tests/{pipeline_path}/test.py"
                await self._store_text_file(test_path, pipeline_data['test_code'])
                stored_files['test_code'] = f"s3://{self.bucket}/{test_path}"

            if 'env_template' in pipeline_data:
                env_path = f"pipeline-code/{pipeline_path}/.env"
                await self._store_text_file(env_path, pipeline_data['env_template'])
                stored_files['.env'] = f"s3://{self.bucket}/{env_path}"

            if 'dockerfile' in pipeline_data:
                dockerfile_path = f"pipeline-code/{pipeline_path}/Dockerfile"
                await self._store_text_file(dockerfile_path, pipeline_data['dockerfile'])
                stored_files['dockerfile'] = f"s3://{self.bucket}/{dockerfile_path}"

            # Store pipeline specification
            if 'spec' in pipeline_data:
                spec_path = f"pipeline-specs/{pipeline_id}/v{timestamp}/spec.json"
                await self._store_json_file(spec_path, pipeline_data['spec'])
                stored_files['spec'] = f"s3://{self.bucket}/{spec_path}"

            # Store test results (if provided)
            if 'test_results' in pipeline_data:
                results_path = f"pipeline-tests/{pipeline_id}/v{timestamp}/test_results.json"
                await self._store_json_file(results_path, pipeline_data['test_results'])
                stored_files['test_results'] = f"s3://{self.bucket}/{results_path}"

            # Store execution logs (if provided)
            if 'logs' in pipeline_data:
                logs_path = f"pipeline-logs/{pipeline_id}/v{timestamp}/execution.log"
                await self._store_text_file(logs_path, pipeline_data['logs'])
                stored_files['logs'] = f"s3://{self.bucket}/{logs_path}"

            # Store metadata
            metadata = {
                "pipeline_id": pipeline_id,
                "version": timestamp,
                "created_at": datetime.now().isoformat(),
                "stored_files": stored_files,
                "file_count": len(stored_files)
            }
            metadata_path = f"pipelines/{pipeline_id}/v{timestamp}/metadata.json"
            await self._store_json_file(metadata_path, metadata)
            stored_files['metadata'] = f"s3://{self.bucket}/{metadata_path}"

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
            metadata_path = f"pipelines/{pipeline_id}/v{version}/metadata.json"
            metadata = await self._retrieve_json_file(metadata_path)

            pipeline_data = {"metadata": metadata}

            # Retrieve all stored files
            for file_type, s3_path in metadata["stored_files"].items():
                if file_type != "metadata":
                    bucket, path = self._parse_s3_path(s3_path)

                    if file_type in ["spec", "test_results"]:
                        pipeline_data[file_type] = await self._retrieve_json_file(path)
                    else:
                        pipeline_data[file_type] = await self._retrieve_text_file(path)

            return pipeline_data

        except Exception as e:
            self.logger.error(f"Error retrieving pipeline: {e}")
            raise

    async def list_pipeline_versions(self, pipeline_id: str) -> List[Dict[str, Any]]:
        """List all versions of a pipeline"""
        try:
            prefix = f"pipelines/{pipeline_id}/"

            # List objects in main bucket
            response = await asyncio.to_thread(
                self.client.list_objects_v2,
                Bucket=self.bucket,
                Prefix=prefix
            )

            versions = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith("/metadata.json"):
                        # Extract version from path: pipelines/{pipeline_id}/v{timestamp}/metadata.json
                        path_parts = obj['Key'].split("/")
                        # Path is: pipelines/{pipeline_id}/v{timestamp}/metadata.json
                        # So indices are: [0]=pipelines, [1]=pipeline_id, [2]=v{timestamp}, [3]=metadata.json
                        if len(path_parts) >= 4 and path_parts[2].startswith("v"):
                            version = path_parts[2][1:]  # Remove 'v' prefix
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
            # Delete from all prefix locations
            if version:
                self.logger.info(f"Deleting pipeline {pipeline_id} version {version}")
                for prefix_name in self.pipeline_prefixes.values():
                    full_prefix = f"{prefix_name}/{pipeline_id}/v{version}/"
                    await self._delete_objects_with_prefix(full_prefix)
            else:
                self.logger.info(f"Deleting all versions of pipeline {pipeline_id}")
                for prefix_name in self.pipeline_prefixes.values():
                    full_prefix = f"{prefix_name}/{pipeline_id}/"
                    await self._delete_objects_with_prefix(full_prefix)

            self.logger.info(f"Deleted pipeline {pipeline_id}" + (f" version {version}" if version else " (all versions)"))

        except Exception as e:
            self.logger.error(f"Error deleting pipeline: {e}")
            raise

    async def _store_text_file(self, path: str, content: str):
        """Store text content as file in main bucket"""
        content_bytes = content.encode('utf-8')

        await asyncio.to_thread(
            self.client.put_object,
            Bucket=self.bucket,
            Key=path,
            Body=content_bytes,
            ContentType='text/plain'
        )

    async def _store_json_file(self, path: str, data: Dict[str, Any]):
        """Store JSON data as file in main bucket"""
        json_content = json.dumps(data, indent=2, default=str)
        await self._store_text_file(path, json_content)

    async def _retrieve_text_file(self, path: str) -> str:
        """Retrieve text file content from main bucket"""
        response = await asyncio.to_thread(
            self.client.get_object,
            Bucket=self.bucket,
            Key=path
        )
        content = response['Body'].read().decode('utf-8')
        return content

    async def _retrieve_json_file(self, path: str) -> Dict[str, Any]:
        """Retrieve JSON file content from main bucket"""
        content = await self._retrieve_text_file(path)
        return json.loads(content)

    async def _get_latest_version(self, pipeline_id: str) -> Optional[str]:
        """Get the latest version of a pipeline"""
        versions = await self.list_pipeline_versions(pipeline_id)
        return versions[0]["version"] if versions else None

    async def _delete_objects_with_prefix(self, prefix: str):
        """Delete all objects with given prefix in main bucket"""
        try:
            response = await asyncio.to_thread(
                self.client.list_objects_v2,
                Bucket=self.bucket,
                Prefix=prefix
            )

            if 'Contents' in response:
                # Delete objects in batches
                objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]

                if objects_to_delete:
                    await asyncio.to_thread(
                        self.client.delete_objects,
                        Bucket=self.bucket,
                        Delete={'Objects': objects_to_delete}
                    )
                    self.logger.debug(f"Deleted {len(objects_to_delete)} objects from {self.bucket}/{prefix}")

        except ClientError as e:
            # Ignore if bucket doesn't exist or is empty
            if e.response['Error']['Code'] not in ['NoSuchBucket', 'NoSuchKey']:
                raise

    def _parse_s3_path(self, s3_path: str) -> tuple:
        """Parse S3 path into bucket and object path"""
        # s3://bucket-name/path/to/object
        parts = s3_path.replace("s3://", "").split("/", 1)
        return parts[0], parts[1] if len(parts) > 1 else ""


    async def get_storage_status(self) -> Dict[str, Any]:
        """Get storage status and bucket information"""
        try:
            prefix_status = {}

            # Check main bucket exists
            try:
                await asyncio.to_thread(self.client.head_bucket, Bucket=self.bucket)
                bucket_exists = True
            except ClientError:
                bucket_exists = False

            # Count objects per prefix
            if bucket_exists:
                for prefix_name, prefix_key in self.pipeline_prefixes.items():
                    try:
                        response = await asyncio.to_thread(
                            self.client.list_objects_v2,
                            Bucket=self.bucket,
                            Prefix=f"{prefix_key}/"
                        )

                        object_count = response.get('KeyCount', 0)
                        total_size = sum(obj.get('Size', 0) for obj in response.get('Contents', []))

                        prefix_status[prefix_name] = {
                            "object_count": object_count,
                            "total_size": total_size
                        }
                    except ClientError as e:
                        prefix_status[prefix_name] = {"error": str(e)}

            return {
                "status": "connected" if bucket_exists else "error",
                "endpoint": self.endpoint if self.endpoint else "AWS S3",
                "bucket": self.bucket,
                "bucket_exists": bucket_exists,
                "prefixes": prefix_status
            }

        except Exception as e:
            self.logger.error(f"Error getting storage status: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
