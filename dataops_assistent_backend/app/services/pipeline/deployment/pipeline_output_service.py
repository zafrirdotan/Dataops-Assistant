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
            
            # Initialize MinIO buckets if needed
            await self.storage_service.initialize_pipeline_buckets()
            
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
        
    def _create_pipeline_files_sync(self, pipeline_name: str, code: str, requirements: str, 
                            python_test: str, output_dir="../pipelines") -> str:
        """Synchronous version for thread pool execution."""
        folder = os.path.abspath(os.path.join(output_dir, pipeline_name))
        os.makedirs(folder, exist_ok=True)
        
        # Create main pipeline file
        code_path = os.path.join(folder, f"{pipeline_name}.py")
        with open(code_path, "w") as f:
            f.write(code)
            
        # Create requirements file
        req_path = os.path.join(folder, "requirements.txt")
        with open(req_path, "w") as f:
            f.write(requirements)
            
        # Create test file
        test_path = os.path.join(folder, f"{pipeline_name}_test.py")
        with open(test_path, "w") as f:
            f.write(python_test)
                
        # Create environment file by copying from template
        env_path = os.path.join(folder, ".env")
        # if os.path.exists(self.env_template_path):
        shutil.copy2(self.env_template_path, env_path)
        # else:
        #     # Fallback to basic content if template doesn't exist
        #     with open(env_path, "w") as f:
        #         f.write("DATA_FOLDER=../../data\n")
            
        return folder
    
    def create_dockerfile(self, folder: str, pipeline_name: str) -> str:
        """
        Creates a Dockerfile for the pipeline.
        
        Args:
            folder: Pipeline folder path
            pipeline_name: Name of the pipeline
            
        Returns:
            str: Path to the created Dockerfile
        """
        dockerfile_path = os.path.join(folder, "Dockerfile")
        dockerfile_content = f"""FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY {pipeline_name}.py .
COPY {pipeline_name}_test.py .
CMD ["python", "{pipeline_name}.py"]
"""
        with open(dockerfile_path, "w") as f:
            f.write(dockerfile_content)
        return dockerfile_path
    
    def get_pipeline_paths(self, folder: str, pipeline_name: str) -> Dict[str, str]:
        """
        Returns a dictionary of all pipeline file paths.
        
        Args:
            folder: Pipeline folder path
            pipeline_name: Name of the pipeline
            
        Returns:
            Dict[str, str]: Dictionary mapping file types to their paths
        """
        return {
            "code": os.path.join(folder, f"{pipeline_name}.py"),
            "requirements": os.path.join(folder, "requirements.txt"),
            "test": os.path.join(folder, f"{pipeline_name}_test.py"),
            "env": os.path.join(folder, ".env"),
            "dockerfile": os.path.join(folder, "Dockerfile")
        }
    
    async def create_pipeline_files_local(self, pipeline_name: str, code: str, requirements: str, 
                            python_test: str, output_dir="../pipelines") -> str:
        """
        Legacy method for local file creation (kept for backward compatibility)
        """
        return await asyncio.to_thread(
            self._create_pipeline_files_sync,
            pipeline_name, code, requirements, python_test, output_dir
        )
    
    def create_pipeline_output(self, pipeline_name: str, code: str, requirements: str, 
                             python_test: str, output_dir="../pipelines") -> str:
        """
        Legacy method name for backward compatibility.
        Delegates to create_pipeline_files.
        """
        import asyncio
        return asyncio.run(self.store_pipeline_files(pipeline_name, code, requirements, python_test, output_dir))
    
    def get_env_template_path(self) -> str:
        """
        Returns the path to the environment template file.
        
        Returns:
            str: Path to the .env.template file
        """
        return self.env_template_path
    
    def update_env_template(self, content: str) -> bool:
        """
        Updates the environment template file with new content.
        
        Args:
            content: New content for the template
            
        Returns:
            bool: True if update was successful, False otherwise
        """
        try:
            with open(self.env_template_path, "w") as f:
                f.write(content)
            return True
        except Exception as e:
            print(f"Error updating .env template: {e}")
            return False
