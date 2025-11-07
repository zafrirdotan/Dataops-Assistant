import os
import sys
import asyncio
import tempfile
import aiofiles
from ..deployment.pipeline_output_service import PipelineOutputService

class PipelineTestService:
    """
    Service responsible for testing pipelines.
    Refactored from TestPipelineService to use PipelineOutputService for file creation.
    """
    
    def __init__(self, log):
        self.log = log
        self.output_service = PipelineOutputService()
        self.env_test_template_path = os.path.join(os.path.dirname(__file__), ".env.test_template")

    async def run_pipeline_test_in_venv(self, pipeline_id: str) -> dict:
        """
        Run the pipeline test in a virtual environment.
        """

        # Step 1: Retrieve pipeline files from MinIO
        try:
            stored_files = await self.output_service.get_pipeline_files(pipeline_id)
            if not stored_files:
                self.log.error(f"No files found for pipeline ID: {pipeline_id}")
                return {"success": False, "details": "No files found for the given pipeline ID."}
        except Exception as e:
            self.log.error(f"Failed to retrieve pipeline files: {e}")
            return {"success": False, "details": f"Failed to retrieve pipeline files: {e}"}
        
        self.log.info(f"Retrieved files for pipeline ID: {pipeline_id}")
            
        # Create temporary directory for execution
        with tempfile.TemporaryDirectory() as temp_dir:
            execution_dir = os.path.join(temp_dir, pipeline_id)
            os.makedirs(execution_dir, exist_ok=True)
            
            # Write pipeline files to temp directory
            pipeline_file = os.path.join(execution_dir, "pipeline.py")
            test_file = os.path.join(execution_dir, "test.py")
            requirements_file = os.path.join(execution_dir, "requirements.txt")
            env_file = os.path.join(execution_dir, ".env")
            
            # Write files asynchronously
            try:
                async with aiofiles.open(pipeline_file, 'w') as f:
                    print("Writing pipeline code:", stored_files.get('pipeline', ''))  # For debugging purposes
                    await f.write(stored_files.get('pipeline', ''))
                
                async with aiofiles.open(test_file, 'w') as f:
                    await f.write(stored_files.get('test_code', ''))
                
                async with aiofiles.open(requirements_file, 'w') as f:
                    await f.write(stored_files.get('requirements', ''))

                with open(self.env_test_template_path, "r") as f:
                    env_test_content = f.read()
                async with aiofiles.open(env_file, 'w') as f:
                    await f.write(env_test_content)

                self.log.info(f"Pipeline files written to temporary directory: {execution_dir}")
            except Exception as e:
                self.log.error(f"Failed to write pipeline files: {e}")
                return {"success": False, "details": f"Failed to write pipeline files: {e}"}
            
            # Create virtual environment
            venv_dir = os.path.join(execution_dir, 'venv')
            try:
                proc = await asyncio.create_subprocess_exec(
                    sys.executable, '-m', 'venv', venv_dir,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                if proc.returncode != 0:
                    self.log.error(f"Failed to create virtual environment: {stderr.decode()}")
                    return {"success": False, "details": f"Failed to create virtual environment: {stderr.decode()}"}
                self.log.info(f"Virtual environment created at {venv_dir}")
            except Exception as e:
                self.log.error(f"Failed to create virtual environment: {e}")
                return {"success": False, "details": f"Failed to create virtual environment: {e}"}
            
            # Install dependencies
            pip_executable = os.path.join(venv_dir, 'bin', 'pip')
            try:
                proc = await asyncio.create_subprocess_exec(
                    pip_executable, 'install', '-r', requirements_file,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                if proc.returncode != 0:
                    self.log.error(f"Failed to install dependencies: {stderr.decode()}")
                    return {"success": False, "details": f"Failed to install dependencies: {stderr.decode()}"}
                self.log.info("Dependencies installed successfully")
            except Exception as e:
                self.log.error(f"Failed to install dependencies: {e}")
                return {"success": False, "details": f"Failed to install dependencies: {e}"}
            
            # Run tests
            pytest_executable = os.path.join(venv_dir, 'bin', 'pytest')
            try:
                proc = await asyncio.create_subprocess_exec(
                    pytest_executable, test_file,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                if proc.returncode == 0:
                    self.log.info("All tests passed successfully")
                else:
                    self.log.error(f"Tests failed:\n{stdout.decode()}\n{stderr.decode()}")
                    return {"success": False, "details": f"Tests failed:\n{stdout.decode()}\n{stderr.decode()}"}
            except Exception as e:
                self.log.error(f"Failed to run tests: {e}")
                return {"success": False, "details": f"Failed to run tests: {e}"}
            
            # Run pipeline code and check for errors in stdout/stderr
            try:
                python_executable = os.path.join(venv_dir, 'bin', 'python')
                proc = await asyncio.create_subprocess_exec(
                    python_executable, pipeline_file,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                    
                stdout, stderr = await proc.communicate()
                stdout_lower = stdout.decode().lower() if stdout else ''
                stderr_lower = stderr.decode().lower() if stderr else ''
                error_keywords = ['error', 'exception', 'traceback', 'fail']
                found_error = any(kw in stdout_lower or kw in stderr_lower for kw in error_keywords)
                
                if proc.returncode == 0 and not found_error:
                    self.log.info("Pipeline executed successfully")
                    return {"success": True, "details": "Pipeline executed successfully"}
                elif found_error:
                    self.log.error(f"Pipeline execution completed but errors detected in output:\n{stdout.decode()}\n{stderr.decode()}")
                    return {"success": False, "details": f"Pipeline execution completed but errors detected in output:\n{stdout.decode()}\n{stderr.decode()}"}
                else:
                    self.log.error(f"Pipeline execution failed:\n{stdout.decode()}\n{stderr.decode()}")
                    return {"success": False, "details": f"Pipeline execution failed:\n{stdout.decode()}\n{stderr.decode()}"}
            except Exception as e:
                self.log.error(f"Failed to execute pipeline: {e}")
                return {"success": False, "details": f"Failed to execute pipeline: {e}"}
            
        return {"success": False, "details": "Unexpected error during pipeline testing."}

