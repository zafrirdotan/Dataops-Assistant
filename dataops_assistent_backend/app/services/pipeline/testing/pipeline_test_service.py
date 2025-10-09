import os
import subprocess
import sys
import asyncio
import tempfile
import logging
import datetime
from typing import Dict, Any, Optional
from ..deployment.pipeline_output_service import PipelineOutputService
from app.services.storage_service import MinioStorage
import aiofiles

class PipelineTestService:
    """
    Service responsible for testing pipelines.
    Now supports both MinIO-based and local file-based testing.
    """
    
    def __init__(self, log):
        self.log = log
        self.output_service = PipelineOutputService()
        self.storage_service = MinioStorage()

    async def run_pipeline_test(self, pipeline_reference: str, pipeline_name: str, execution_mode="venv") -> dict:
        """
        Run pipeline test by downloading from MinIO or using local files.
        
        Args:
            pipeline_reference: Can be pipeline_id (for MinIO) or folder path (for local)
            pipeline_name: Name of the pipeline
            execution_mode: Either 'venv' or 'docker'
            
        Returns:
            dict: Test result with success status and details
        """
        
        # Check if this is a MinIO reference (starts with minio://) or contains pipeline_id pattern
        if pipeline_reference.startswith("minio://") or self._is_pipeline_id(pipeline_reference):
            return await self._run_test_from_minio(pipeline_reference, pipeline_name, execution_mode)
        else:
            # Fallback to original local file execution
            return await self._run_test_from_local(pipeline_reference, pipeline_name, execution_mode)

    def _is_pipeline_id(self, reference: str) -> bool:
        """Check if the reference looks like a pipeline_id rather than a file path"""
        # Pipeline IDs typically have timestamp and UUID format like: name_20251009_HHMMSS_uuid
        return len(reference.split('_')) >= 3 and not reference.startswith('/') and not os.path.exists(reference)

    async def _run_test_from_minio(self, pipeline_reference: str, pipeline_name: str, execution_mode: str) -> Dict[str, Any]:
        """Download pipeline from MinIO and execute in temporary directory"""
        
        try:
            # Extract pipeline_id from reference
            if pipeline_reference.startswith("minio://"):
                pipeline_id = pipeline_reference.replace("minio://pipelines/", "")
            else:
                pipeline_id = pipeline_reference
            
            self.log.info(f"Downloading pipeline {pipeline_id} from MinIO for testing...")
            
            # Retrieve pipeline from MinIO
            try:
                pipeline_data = await self.storage_service.retrieve_pipeline(pipeline_id)
            except Exception as e:
                self.log.error(f"Failed to retrieve pipeline from MinIO: {e}")
                return {"success": False, "details": f"Failed to retrieve pipeline from MinIO: {e}"}
            
            # Create temporary directory for execution
            with tempfile.TemporaryDirectory() as temp_dir:
                execution_dir = os.path.join(temp_dir, pipeline_id)
                os.makedirs(execution_dir, exist_ok=True)
                
                # Write pipeline files to temp directory
                pipeline_file = os.path.join(execution_dir, f"{pipeline_name}.py")
                test_file = os.path.join(execution_dir, f"{pipeline_name}_test.py")
                requirements_file = os.path.join(execution_dir, "requirements.txt")
                
                # Write files asynchronously
                try:
                    async with aiofiles.open(pipeline_file, 'w') as f:
                        await f.write(pipeline_data.get('code', ''))
                    
                    async with aiofiles.open(test_file, 'w') as f:
                        await f.write(pipeline_data.get('test_code', ''))
                    
                    async with aiofiles.open(requirements_file, 'w') as f:
                        await f.write(pipeline_data.get('requirements', ''))
                    
                    self.log.info(f"Pipeline files written to temporary directory: {execution_dir}")
                except Exception as e:
                    self.log.error(f"Failed to write pipeline files: {e}")
                    return {"success": False, "details": f"Failed to write pipeline files: {e}"}
                
                # Execute tests in the temporary directory
                if execution_mode == "venv":
                    result = await self._execute_in_venv(execution_dir, pipeline_name)
                elif execution_mode == "docker":
                    result = await self._execute_in_docker(execution_dir, pipeline_name)
                else:
                    result = {"success": False, "details": "Unknown execution mode"}
                
                # Store execution results back to MinIO
                await self._store_execution_results(pipeline_id, result)
                
                return result
                
        except Exception as e:
            self.log.error(f"Error running pipeline test from MinIO: {e}")
            return {"success": False, "details": f"Pipeline test execution failed: {e}"}

    async def _run_test_from_local(self, folder_path: str, pipeline_name: str, execution_mode: str) -> Dict[str, Any]:
        """Original method for running tests from local files (backward compatibility)"""
        
        self.log.info(f"Running pipeline test for {pipeline_name} from local files...")
        
        paths = self.output_service.get_pipeline_paths(folder_path, pipeline_name)
        
        if execution_mode == "venv":
            return await self._run_venv_test(folder_path, pipeline_name, paths)
        elif execution_mode == "docker":
            return await self._run_docker_test(folder_path, pipeline_name, paths)
        else:
            return {"success": False, "details": "Unknown execution mode."}

    async def _run_venv_test(self, folder: str, pipeline_name: str, paths: dict) -> dict:
        """
        Runs tests using virtual environment.
        
        Args:
            folder: Path to the pipeline folder
            pipeline_name: Name of the pipeline
            paths: Dictionary of file paths
            
        Returns:
            dict: Test result with success status and details
        """
        try: 
            venv_path = os.path.join(folder, "venv")
            # Create virtual environment and wait for completion
            process = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "venv", venv_path
            )
            await process.wait()
            
            # Use appropriate paths for Linux containers
            pip_path = os.path.join(venv_path, "bin", "pip")
            python_path = os.path.join(venv_path, "bin", "python")
            
            # Verify the virtual environment was created successfully
            if not os.path.exists(pip_path):
                self.log.error(f"Virtual environment creation failed. Pip not found at: {pip_path}")
                return {"success": False, "details": f"Virtual environment creation failed. Pip not found at: {pip_path}"}
            
            # Install requirements
            process = await asyncio.create_subprocess_exec(
                pip_path, "install", "-r", paths["requirements"]
            )
            await process.wait()
            
            # Run main pipeline
            process = await asyncio.create_subprocess_exec(
                python_path, paths["code"],
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=folder
            )
            stdout, stderr = await process.communicate()
            
            self.log.info(f"Pipeline test completed for {pipeline_name} with return code {process.returncode}.")
            
            if process.returncode != 0:
                self.log.error(f"Pipeline test failed for {pipeline_name} with error: {stderr.decode()}")
                return {"success": False, "details": stderr.decode()}
            
            # Run unit tests
            return await self._run_unit_tests(python_path, paths["test"], folder, pipeline_name)
            
        except Exception as e:
            self.log.error(f"Error occurred while running pipeline test: {e}")
            return {"success": False, "details": str(e)}

    async def _run_docker_test(self, folder: str, pipeline_name: str, paths: dict) -> dict:
        """
        Runs tests using Docker.
        
        Args:
            folder: Path to the pipeline folder
            pipeline_name: Name of the pipeline
            paths: Dictionary of file paths
            
        Returns:
            dict: Test result with success status and details
        """
        try:
            self.output_service.create_dockerfile(folder, pipeline_name)
            image_tag = f"{pipeline_name}_test_image"
            
            # Build docker image
            process = await asyncio.create_subprocess_exec(
                "docker", "build", "-t", image_tag, folder
            )
            await process.wait()
            
            # Run docker container
            process = await asyncio.create_subprocess_exec(
                "docker", "run", "--rm", image_tag,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            return {
                "success": process.returncode == 0,
                "stdout": stdout.decode(),
                "stderr": stderr.decode()
            }
        except Exception as e:
            return {"success": False, "details": str(e)}

    async def _run_unit_tests(self, python_path: str, test_path: str, folder: str, pipeline_name: str) -> dict:
        """
        Runs unit tests using pytest.
        
        Args:
            python_path: Path to Python executable
            test_path: Path to test file
            folder: Pipeline folder
            pipeline_name: Name of the pipeline
            
        Returns:
            dict: Test result with success status and details
        """
        try:
            process = await asyncio.create_subprocess_exec(
                python_path, "-m", "pytest", test_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=folder
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                self.log.error(f"Unit test failed for {pipeline_name} with error: {stderr.decode()} and stdout: {stdout.decode()}")
                return {
                    "success": False, 
                    "details": f"Unit test failed with error: {stderr.decode()}, stdout: {stdout.decode()}"
                }

            self.log.info(f"Unit test executed successfully for {pipeline_name} with output: {stdout.decode()}")
            return {
                "success": True, 
                "details": "Unit test executed successfully.", 
                "stdout": stdout.decode()
            }
        except Exception as e:
            self.log.error(f"Unit test failed for {pipeline_name} with exception: {e}")
            return {"success": False, "details": str(e)}

    async def _execute_in_venv(self, execution_dir: str, pipeline_name: str) -> Dict[str, Any]:
        """Execute pipeline in a virtual environment (for MinIO-based testing)"""
        
        try:
            venv_path = os.path.join(execution_dir, "venv")
            
            # Create virtual environment
            self.log.info(f"Creating virtual environment at: {venv_path}")
            process = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "venv", venv_path
            )
            await process.wait()
            
            # Verify venv creation
            pip_path = os.path.join(venv_path, "bin", "pip")
            python_path = os.path.join(venv_path, "bin", "python")
            
            if not os.path.exists(pip_path):
                return {"success": False, "details": f"Virtual environment creation failed at {venv_path}"}
            
            # Install dependencies
            requirements_file = os.path.join(execution_dir, "requirements.txt")
            if os.path.exists(requirements_file):
                self.log.info("Installing dependencies...")
                install_process = await asyncio.create_subprocess_exec(
                    pip_path, "install", "-r", requirements_file,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                await install_process.wait()
            
            # Run tests
            test_file = os.path.join(execution_dir, f"{pipeline_name}_test.py")
            
            self.log.info(f"Running tests with: {python_path} -m pytest {test_file} -v")
            
            process = await asyncio.create_subprocess_exec(
                python_path, "-m", "pytest", test_file, "-v",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=execution_dir
            )
            
            stdout, stderr = await process.communicate()
            
            output = stdout.decode('utf-8') + stderr.decode('utf-8')
            success = process.returncode == 0
            
            self.log.info(f"Pipeline test completed for {pipeline_name} with return code {process.returncode}")
            
            return {
                "success": success,
                "return_code": process.returncode,
                "output": output,
                "details": f"Pipeline test {'passed' if success else 'failed'}"
            }
            
        except Exception as e:
            self.log.error(f"Error in venv execution: {e}")
            return {"success": False, "details": f"venv execution failed: {e}"}

    async def _execute_in_docker(self, execution_dir: str, pipeline_name: str) -> Dict[str, Any]:
        """Execute pipeline in Docker container (for MinIO-based testing)"""
        
        try:
            # Create Dockerfile
            self.output_service.create_dockerfile(execution_dir, pipeline_name)
            image_tag = f"{pipeline_name}_test_image"
            
            # Build docker image
            build_process = await asyncio.create_subprocess_exec(
                "docker", "build", "-t", image_tag, execution_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await build_process.wait()
            
            if build_process.returncode != 0:
                return {"success": False, "details": "Docker build failed"}
            
            # Run docker container
            run_process = await asyncio.create_subprocess_exec(
                "docker", "run", "--rm", image_tag,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await run_process.communicate()
            
            success = run_process.returncode == 0
            
            return {
                "success": success,
                "return_code": run_process.returncode,
                "stdout": stdout.decode(),
                "stderr": stderr.decode(),
                "details": f"Docker test {'passed' if success else 'failed'}"
            }
            
        except Exception as e:
            self.log.error(f"Error in Docker execution: {e}")
            return {"success": False, "details": f"Docker execution failed: {e}"}

    async def _store_execution_results(self, pipeline_id: str, execution_result: Dict[str, Any]):
        """Store execution results back to MinIO"""
        try:
            execution_log = {
                "pipeline_id": pipeline_id,
                "execution_time": datetime.datetime.now().isoformat(),
                "success": execution_result.get("success", False),
                "return_code": execution_result.get("return_code", -1),
                "output": execution_result.get("output", ""),
                "details": execution_result.get("details", ""),
                "mode": "test_execution"
            }
            
            # Add execution log to pipeline data
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            log_path = f"{pipeline_id}/executions/{timestamp}_test_execution.json"
            
            await self.storage_service._store_json_file("pipeline-logs", log_path, execution_log)
            self.log.info(f"Execution results stored for pipeline {pipeline_id}")
            
        except Exception as e:
            self.log.error(f"Failed to store execution results: {e}")

