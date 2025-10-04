import os
from typing import Dict

class PipelineOutputService:
    """
    Service responsible for creating and managing pipeline output files.
    Extracted from TestPipelineService to separate concerns.
    """
    
    def __init__(self):
        pass
    
    def create_pipeline_files(self, pipeline_name: str, code: str, requirements: str, 
                            python_test: str, output_dir="../pipelines") -> str:
        """
        Creates all necessary pipeline files in a dedicated folder.
        
        Args:
            pipeline_name: Name of the pipeline
            code: Python code for the pipeline
            requirements: Requirements.txt content
            python_test: Test code for the pipeline
            output_dir: Base directory for pipeline outputs
            
        Returns:
            str: Path to the created pipeline folder
        """
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
                
        # Create environment file
        env_path = os.path.join(folder, ".env")
        with open(env_path, "w") as f:
            f.write("DATA_FOLDER=../../data\n")
            
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
    
    def create_pipeline_output(self, pipeline_name: str, code: str, requirements: str, 
                             python_test: str, output_dir="../pipelines") -> str:
        """
        Legacy method name for backward compatibility.
        Delegates to create_pipeline_files.
        """
        return self.create_pipeline_files(pipeline_name, code, requirements, python_test, output_dir)
