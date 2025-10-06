import os
import subprocess
import sys
import asyncio
from ..deployment.pipeline_output_service import PipelineOutputService

class PipelineTestService:
    """
    Service responsible for testing pipelines.
    Refactored from TestPipelineService to use PipelineOutputService for file creation.
    """
    
    def __init__(self, log):
        self.log = log
        self.output_service = PipelineOutputService()

    async def run_pipeline_test(self, folder: str, pipeline_name: str, execution_mode="venv") -> dict:
        """
        Runs tests for a pipeline in the specified folder.
        
        Args:
            folder: Path to the pipeline folder
            pipeline_name: Name of the pipeline
            execution_mode: Either 'venv' or 'docker'
            
        Returns:
            dict: Test result with success status and details
        """
        self.log.info(f"Running pipeline test for {pipeline_name}...")
        
        paths = self.output_service.get_pipeline_paths(folder, pipeline_name)
        
        if execution_mode == "venv":
            return await self._run_venv_test(folder, pipeline_name, paths)
        elif execution_mode == "docker":
            return await self._run_docker_test(folder, pipeline_name, paths)
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

