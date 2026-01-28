import os

import logging
import datetime
import uuid
import json
from typing import Dict, Any
from shared.services.storage_factory import get_storage_service
from ..types import CodeGenResult


class PipelineOutputService:
    """
    Service responsible for creating and managing pipeline output files.
    Storage location depends on ENVIRONMENT variable:
    - prod/production: AWS S3
    - dev/development: MinIO
    - local-debug/local: Local filesystem
    """

    def __init__(self):
        self.log = logging.getLogger("dataops")
        self.storage_service = get_storage_service()
        # Keep template dir for backward compatibility
        self.template_dir = os.path.dirname(__file__)
        self.env_template_path = os.path.join(self.template_dir, ".env.template")
        self.dockerfile_template_path = os.path.join(self.template_dir, "Dockerfile.template")

    async def store_pipeline_files(self, pipeline_name: str, code: CodeGenResult) -> Dict[str, Any]:
        """
        Creates pipeline files and stores them using the configured storage service.
        Storage location is determined by ENVIRONMENT variable.

        Args:
            pipeline_name: Name of the pipeline
            code: Python code for the pipeline

        Returns:
            Dict[str, Any]: Pipeline metadata including storage locations
        """
        try:
            # Generate unique pipeline ID
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            unique_id = str(uuid.uuid4())[:8]
            pipeline_id = f"{pipeline_name}_{timestamp}_{unique_id}"

            self.log.info(f"Creating pipeline files for: {pipeline_id}")

            # Prepare pipeline data for MinIO storage

            def ensure_str(val):
                if isinstance(val, dict):
                    return json.dumps(val, indent=2)
                return str(val)

            pipeline_data = {
                "pipeline": ensure_str(code["pipeline"]),
                "test_code": ensure_str(code["tests"]),
                "requirements": ensure_str(code["requirements"]),
                "created_at": datetime.datetime.now().isoformat(),
                "pipeline_name": pipeline_name,
                "env_template": self.get_env_as_string(),
                "dockerfile": self.get_dockerfile_as_string(),
            }

            # Store pipeline using configured storage service
            storage_type = type(self.storage_service).__name__
            self.log.info(f"Storing pipeline {pipeline_id} using {storage_type}...")
            stored_files = await self.storage_service.store_pipeline(pipeline_id, pipeline_data)

            self.log.info(f"Pipeline {pipeline_id} stored successfully using {storage_type}")

            # Return pipeline metadata
            return {
                "success": True,
                "pipeline_id": pipeline_id,
                "storage_type": storage_type,
                "stored_files": stored_files,
                "folder": f"pipelines/{pipeline_id}",
                "timestamp": timestamp
            }

        except Exception as e:
            self.log.error(f"Failed to create pipeline files: {e}")
            raise Exception(f"Pipeline file creation failed: {e}")

    def get_env_as_string(self) -> str:
        """
        Reads the .env.template file and returns its content as a string.

        Returns:
            str: Content of the .env.template file
        """
        try:
            with open(self.env_template_path, "r") as f:
                return f.read()
        except Exception as e:
            self.log.error(f"Failed to read .env template: {e}")

    def get_dockerfile_as_string(self) -> str:
        """
        Reads the Dockerfile.template file and returns its content as a string.

        Returns:
            str: Content of the Dockerfile.template file
        """
        try:
            with open(self.dockerfile_template_path, "r") as f:
                return f.read()
        except Exception as e:
            self.log.error(f"Failed to read Dockerfile template: {e}")

    async def get_pipeline_files(self, pipeline_id: str) -> Dict[str, str]:
        """
        Retrieves the pipeline files from configured storage service.

        Args:
            pipeline_id: Unique ID of the pipeline
        Returns:
            Dict[str, str]: Dictionary containing the content of the pipeline files
        """
        try:
            stored_files = await self.storage_service.retrieve_pipeline(pipeline_id)
            if not stored_files:
                self.log.error(f"No files found for pipeline ID: {pipeline_id}")
                return {}
            return stored_files
        except Exception as e:
            self.log.error(f"Failed to retrieve pipeline files: {e}")
            return {}
