import os
import subprocess
import sys
from ..deployment.pipeline_output_service import PipelineOutputService

class PipelineTestService:
    """
    Service responsible for testing pipelines.
    Refactored from TestPipelineService to use PipelineOutputService for file creation.
    """
    
    def __init__(self, log):
        self.log = log
        self.output_service = PipelineOutputService()

    def run_pipeline_test(self, folder: str, pipeline_name: str, execution_mode="venv") -> dict:
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
            return self._run_venv_test(folder, pipeline_name, paths)
        elif execution_mode == "docker":
            return self._run_docker_test(folder, pipeline_name, paths)
        else:
            return {"success": False, "details": "Unknown execution mode."}

    def _run_venv_test(self, folder: str, pipeline_name: str, paths: dict) -> dict:
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
            subprocess.run([sys.executable, "-m", "venv", venv_path], check=True)
            
            pip_path = os.path.join(venv_path, "bin", "pip")
            python_path = os.path.join(venv_path, "bin", "python")
            
            # Install requirements
            subprocess.run([pip_path, "install", "-r", paths["requirements"]], check=True)
            
            # Run main pipeline
            result = subprocess.run([python_path, paths["code"]], capture_output=True, text=True, cwd=folder)
            self.log.info(f"Pipeline test completed for {pipeline_name} with return code {result.returncode}.")
            
            if result.returncode != 0:
                self.log.error(f"Pipeline test failed for {pipeline_name} with error: {result.stderr}")
                return {"success": False, "details": result.stderr}
            
            # Run unit tests
            return self._run_unit_tests(python_path, paths["test"], folder, pipeline_name)
            
        except Exception as e:
            self.log.error(f"Error occurred while running pipeline test: {e}")
            return {"success": False, "details": str(e)}

    def _run_docker_test(self, folder: str, pipeline_name: str, paths: dict) -> dict:
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
            
            subprocess.run(["docker", "build", "-t", image_tag, folder], check=True)
            result = subprocess.run(["docker", "run", "--rm", image_tag], capture_output=True, text=True)
            
            return {
                "success": result.returncode == 0,
                "stdout": result.stdout,
                "stderr": result.stderr
            }
        except Exception as e:
            return {"success": False, "details": str(e)}

    def _run_unit_tests(self, python_path: str, test_path: str, folder: str, pipeline_name: str) -> dict:
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
            test_result = subprocess.run(
                [python_path, "-m", "pytest", test_path],
                capture_output=True,
                text=True,
                cwd=folder
            )
            
            if test_result.returncode != 0:
                self.log.error(f"Unit test failed for {pipeline_name} with error: {test_result.stderr} and stdout: {test_result.stdout}")
                return {
                    "success": False, 
                    "details": f"Unit test failed with error: {test_result.stderr}, stdout: {test_result.stdout}"
                }

            self.log.info(f"Unit test executed successfully for {pipeline_name} with output: {test_result.stdout}")
            return {
                "success": True, 
                "details": "Unit test executed successfully.", 
                "stdout": test_result.stdout
            }
        except Exception as e:
            self.log.error(f"Unit test failed for {pipeline_name} with exception: {e}")
            return {"success": False, "details": str(e)}

    def create_and_run_unittest(self, name: str, code: str, requirements: str, 
                              python_test: str, execution_mode="venv") -> dict:
        """
        Creates pipeline files and runs unit tests.
        
        Args:
            name: Pipeline name
            code: Python code for the pipeline
            requirements: Requirements.txt content
            python_test: Test code
            execution_mode: Either 'venv' or 'docker'
            
        Returns:
            dict: Test result with success status and details
        """
        folder = self.output_service.create_pipeline_files(name, code, requirements, python_test)
        return self.run_pipeline_test(folder, name, execution_mode)
