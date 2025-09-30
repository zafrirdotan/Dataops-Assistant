import os
import subprocess
import sys
import runpy

class TestPipelineService:
    def __init__(self, log):
            self.log = log

    def create_pipeline_output(self, pipeline_name: str, code: str, requirements: str, python_test: str, output_dir="../pipelines") -> str:
        folder = os.path.abspath(os.path.join(output_dir, pipeline_name))
        os.makedirs(folder, exist_ok=True)
        code_path = os.path.join(folder, f"{pipeline_name}.py")
        req_path = os.path.join(folder, "requirements.txt")
        test_path = os.path.join(folder, f"{pipeline_name}_test.py")
        env_path = os.path.join(folder, ".env")

        with open(code_path, "w") as f:
            f.write(code)
        with open(req_path, "w") as f:
            f.write(requirements)
        with open(test_path, "w") as f:
            f.write(python_test)
        with open(env_path, "w") as f:
            f.write("DATA_FOLDER=../../data\n")
        return folder

    def run_pipeline_test(self, folder: str, pipeline_name: str, execution_mode="venv") -> dict:
        self.log.info(f"Running pipeline test for {pipeline_name}...")
        code_path = os.path.join(folder, f"{pipeline_name}.py")
        req_path = os.path.join(folder, "requirements.txt")
        if execution_mode == "venv":
            try: 
                venv_path = os.path.join(folder, "venv")
                subprocess.run([sys.executable, "-m", "venv", venv_path], check=True)
                pip_path = os.path.join(venv_path, "bin", "pip")
                python_path = os.path.join(venv_path, "bin", "python")
                subprocess.run([pip_path, "install", "-r", req_path], check=True)
                result = subprocess.run([python_path, code_path], capture_output=True, text=True, cwd=folder)
                self.log.info(f"Pipeline test completed for {pipeline_name} with return code {result.returncode}.")
                if result.returncode != 0:
                    self.log.error(f"Pipeline test failed for {pipeline_name} with error: {result.stderr}")
                    return {"success": False, "details": result.stderr}
                
                # Run test to verify the output of the main transformation function
                test_path = os.path.join(folder, f"{pipeline_name}_test.py")
                try:
                    test_result = subprocess.run(
                        [python_path, "-m", "pytest", test_path],
                        capture_output=True,
                        text=True,
                        cwd=folder
                    )
                    if test_result.returncode != 0:
                        self.log.error(f"Unit test failed for {pipeline_name} with error: {test_result.stderr} and stdout: {test_result.stdout}")
                        return {"success": False, "details": f"Unit test failed with error: {test_result.stderr}, stdout: {test_result.stdout}"}

                    self.log.info(f"Unit test executed successfully for {pipeline_name} with output: {test_result.stdout}")
                    return {"success": True, "details": "Unit test executed successfully.", "stdout": test_result.stdout}
                except Exception as e:
                    self.log.error(f"Unit test failed for {pipeline_name} with exception: {e}")
                    return {"success": False, "details": str(e)}
            except Exception as e:
                self.log.error(f"Error occurred while running pipeline test: {e}")
                return {"success": False, "details": str(e)}
        elif execution_mode == "docker":
            dockerfile_path = os.path.join(folder, "Dockerfile")
            dockerfile_content = f"""
                FROM python:3.11-slim
                WORKDIR /app
                COPY requirements.txt .
                RUN pip install --no-cache-dir -r requirements.txt
                COPY {pipeline_name}_test.py .
                CMD ["python", "{pipeline_name}_test.py"]
                """
            with open(dockerfile_path, "w") as f:
                f.write(dockerfile_content)
            image_tag = f"{pipeline_name}_test_image"
            subprocess.run(["docker", "build", "-t", image_tag, folder], check=True)
            result = subprocess.run(["docker", "run", "--rm", image_tag], capture_output=True, text=True)
        else:
            return {"success": False, "details": "Unknown execution mode."}
        return {
            "success": result.returncode == 0,
            "stdout": result.stdout,
            "stderr": result.stderr
        }

    def create_and_run_unittest(self, name: str, code: str, requirements: str, python_test: str, execution_mode="venv") -> dict:
        folder = self.create_pipeline_output(name, code, requirements, python_test)
        return self.run_pipeline_test(folder, name, execution_mode)