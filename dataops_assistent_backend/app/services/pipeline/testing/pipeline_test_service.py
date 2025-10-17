import os
import subprocess
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
        self.log.info(f"Stored files content: {stored_files}")

            

        pipeline_name = pipeline_id.split('_')[0]  # Assuming pipeline_id format is <name>_<unique>
            
        # Create temporary directory for execution
        with tempfile.TemporaryDirectory() as temp_dir:
            execution_dir = os.path.join(temp_dir, pipeline_id)
            os.makedirs(execution_dir, exist_ok=True)
            
            # Write pipeline files to temp directory
            pipeline_file = os.path.join(execution_dir, f"{pipeline_name}.py")
            test_file = os.path.join(execution_dir, f"{pipeline_name}_test.py")
            requirements_file = os.path.join(execution_dir, "requirements.txt")
            env_file = os.path.join(execution_dir, ".env")
            
            # log all stored_files data to debug
            self.log.info(f"Stored files content: {stored_files}")

            # Write files asynchronously
            try:
                async with aiofiles.open(pipeline_file, 'w') as f:
                    await f.write(stored_files.get('code', ''))
                
                async with aiofiles.open(test_file, 'w') as f:
                    await f.write(stored_files.get('test_code', ''))
                
                async with aiofiles.open(requirements_file, 'w') as f:
                    await f.write(stored_files.get('requirements', ''))

                async with aiofiles.open(env_file, 'w') as f:
                    await f.write(stored_files.get('env', ''))
                
                self.log.info(f"Pipeline files written to temporary directory: {execution_dir}")
            except Exception as e:
                self.log.error(f"Failed to write pipeline files: {e}")
                return {"success": False, "details": f"Failed to write pipeline files: {e}"}
            
            # Create virtual environment
            venv_dir = os.path.join(execution_dir, 'venv')
            try:
                subprocess.run([sys.executable, '-m', 'venv', venv_dir], check=True)
                self.log.info(f"Virtual environment created at {venv_dir}")
            except subprocess.CalledProcessError as e:
                self.log.error(f"Failed to create virtual environment: {e}")
                return {"success": False, "details": f"Failed to create virtual environment: {e}"}
            
            # Install dependencies
            pip_executable = os.path.join(venv_dir, 'bin', 'pip')
            try:
                subprocess.run([pip_executable, 'install', '-r', requirements_file], check=True)
                self.log.info("Dependencies installed successfully")
            except subprocess.CalledProcessError as e:
                self.log.error(f"Failed to install dependencies: {e}")
                return {"success": False, "details": f"Failed to install dependencies: {e}"}
            
            # Run tests
            pytest_executable = os.path.join(venv_dir, 'bin', 'pytest')
            try:
                result = subprocess.run([pytest_executable, test_file], capture_output=True, text=True)
                if result.returncode == 0:
                    self.log.info("All tests passed successfully")
                else:
                    self.log.error(f"Tests failed:\n{result.stdout}\n{result.stderr}")
                    return {"success": False, "details": f"Tests failed:\n{result.stdout}\n{result.stderr}"}
            except subprocess.CalledProcessError as e:
                self.log.error(f"Failed to run tests: {e}")
                return {"success": False, "details": f"Failed to run tests: {e}"}
            
            # Run pipeline code and check for errors in stdout/stderr
            try:
                python_executable = os.path.join(venv_dir, 'bin', 'python')
                result = subprocess.run([python_executable, pipeline_file], capture_output=True, text=True)
                stdout_lower = result.stdout.lower() if result.stdout else ''
                stderr_lower = result.stderr.lower() if result.stderr else ''
                error_keywords = ['error', 'exception', 'traceback', 'fail']
                found_error = any(kw in stdout_lower or kw in stderr_lower for kw in error_keywords)
                if result.returncode == 0 and not found_error:
                    self.log.info("Pipeline executed successfully")
                    return {"success": True, "details": "Pipeline executed successfully"}
                elif found_error:
                    self.log.error(f"Pipeline execution completed but errors detected in output:\n{result.stdout}\n{result.stderr}")
                    return {"success": False, "details": f"Pipeline execution completed but errors detected in output:\n{result.stdout}\n{result.stderr}"}
                else:
                    self.log.error(f"Pipeline execution failed:\n{result.stdout}\n{result.stderr}")
                    return {"success": False, "details": f"Pipeline execution failed:\n{result.stdout}\n{result.stderr}"}
            except subprocess.CalledProcessError as e:
                self.log.error(f"Failed to execute pipeline: {e}")
                return {"success": False, "details": f"Failed to execute pipeline: {e}"}
            
        return {"success": False, "details": "Unexpected error during pipeline testing."}
   




            

    
