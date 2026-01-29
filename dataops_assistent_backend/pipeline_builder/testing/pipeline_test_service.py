from calendar import c
import json
import os
import asyncio
import tempfile
import aiofiles

from ..deployment.pipeline_output_service import PipelineOutputService
from pipeline_builder.generators.pipeline_code_generator_LLM_hybrid import PipelineCodeGeneratorLLMHybrid
# Write requirements.txt for reference (not used for venv install)
from pipeline_builder.generators.pipeline_code_generator_LLM_hybrid import PipelineCodeGeneratorLLMHybrid
class PipelineTestService:
    """
    Service responsible for testing pipelines.
    Refactored from TestPipelineService to use PipelineOutputService for file creation.
    """

    def __init__(self, log):
        self.log = log
        self.output_service = PipelineOutputService()
        self.env_test_template_path = os.path.join(os.path.dirname(__file__), ".env.test_template")

    async def run_pipeline_test_in_venv_v2(self, pipeline_id: str) -> dict:
        """
        Run the pipeline test in a single shared virtual environment for all pipelines.
        Assumes the venv is created by entrypoint.sh at /app/.venvs/shared.
        """

        venv_dir = "/app/.venvs/shared"

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
            meta_file = os.path.join(execution_dir, "metadata.json")
            test_file = os.path.join(execution_dir, "test.py")
            requirements_file = os.path.join(execution_dir, "requirements.txt")
            env_file = os.path.join(execution_dir, ".env")

            try:
                async with aiofiles.open(pipeline_file, 'w') as f:
                    await f.write(stored_files.get('pipeline', ''))

                metadata_content = json.dumps(stored_files.get('metadata', ''), indent=2)
                async with aiofiles.open(meta_file, 'w') as f:
                    await f.write(metadata_content)

                async with aiofiles.open(test_file, 'w') as f:
                    await f.write(stored_files.get('test_code', ''))

                with open(self.env_test_template_path, "r") as f:
                    env_test_content = f.read()
                # Optionally, you can set S3 credentials here if needed
                if (os.getenv('ENVIRONMENT') == 'prod'):
                    env_test_content = self.addS3Credentials(env_test_content)
                print("ENV TEST CONTENT:", env_test_content)
                async with aiofiles.open(env_file, 'w') as f:
                    await f.write(env_test_content)

                self.log.info(f"Pipeline files written to temporary directory: {execution_dir}")
            except Exception as e:
                self.log.error(f"Failed to write pipeline files: {e}")
                return {"success": False, "details": f"Failed to write pipeline files: {e}"}

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

    def addS3Credentials(self, env_test_content):
        """
        Retrieves S3 credentials from environment variables for use in .env.test file.
        """
        credentials = {
            "S3_ENDPOINT": os.getenv("S3_ENDPOINT", "http://minio:9000"),
            "S3_ACCESS_KEY": os.getenv("S3_ACCESS_KEY", "minioadmin"),
            "S3_SECRET_KEY": os.getenv("S3_SECRET_KEY", "minioadmin"),
            "S3_REGION": os.getenv("S3_REGION", "us-east-1"),
            "S3_BUCKET": os.getenv("S3_BUCKET", "dataops-pipelines"),
            "S3_USE_PATH_STYLE": os.getenv("S3_USE_PATH_STYLE", "true"),
            "PUBLIC_S3_BASE_URL": os.getenv("PUBLIC_S3_BASE_URL", ""),
            "PRESIGN_EXPIRES_SECONDS": os.getenv("PRESIGN_EXPIRES_SECONDS", "600"),

        }

        for key, value in credentials.items():
            env_test_content += f"\n{key}={value}"

        return env_test_content
