import os
import shutil
import asyncio
import logging
import datetime
import uuid
import json
from typing import Dict, Any
from app.services.storage_service import MinioStorage
from ..types import CodeGenResult

class PipelineOutputService:
    """
    Service responsible for creating and managing pipeline output files.
    Now stores files directly in MinIO instead of local filesystem.
    """
    
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.storage_service = MinioStorage()
        # Keep template dir for backward compatibility
        self.template_dir = os.path.dirname(__file__)
        self.env_template_path = os.path.join(self.template_dir, ".env.template")
    
    async def store_pipeline_files(self, pipeline_name: str, code: CodeGenResult) -> Dict[str, Any]:
        """
        Creates pipeline files and stores them directly in MinIO instead of local filesystem.
        
        Args:
            pipeline_name: Name of the pipeline
            code: Python code for the pipeline
            requirements: Requirements.txt content
            python_test: Test code for the pipeline
            output_dir: Base directory (ignored, kept for compatibility)
            
        Returns:
            Dict[str, Any]: Pipeline metadata including MinIO storage locations
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
                "code": ensure_str(code["code"]),
                "test_code": ensure_str(code["tests"]),
                "requirements": ensure_str(code["requirements"]),
                "created_at": datetime.datetime.now().isoformat(),
                "pipeline_name": pipeline_name,
                "env_template": self.get_env_as_string()
            }
            
            # Store pipeline directly in MinIO
            self.log.info(f"Storing pipeline {pipeline_id} in MinIO...")
            stored_files = await self.storage_service.store_pipeline(pipeline_id, pipeline_data)
            
            self.log.info(f"Pipeline {pipeline_id} stored successfully in MinIO")
            
            # Return pipeline metadata (compatible with old interface)
            return {
                "success": True,
                "pipeline_id": pipeline_id,
                "storage_location": "MinIO",
                "stored_files": stored_files,
                "folder": f"minio://pipelines/{pipeline_id}",  # Virtual folder path for compatibility
                "timestamp": timestamp
            }
            
        except Exception as e:
            self.log.error(f"Failed to create pipeline files in MinIO: {e}")
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

    def get_pipeline_files(self, pipeline_id: str) -> Dict[str, str]:
        """
        Retrieves the pipeline files from MinIO storage.
        
        Args:
            pipeline_id: Unique ID of the pipeline
        Returns:
            Dict[str, str]: Dictionary containing the content of the pipeline files
        """
        try:
            stored_files = self.storage_service.retrieve_pipeline(pipeline_id)
            if not stored_files:
                self.log.error(f"No files found for pipeline ID: {pipeline_id}")
                return {}
            return stored_files
        except Exception as e:
            self.log.error(f"Failed to retrieve pipeline files from MinIO: {e}")
            return {}    
