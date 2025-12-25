
# step8.py
import json
import logging
from pipeline_builder.deployment.dockerize_service import DockerizeService
from shared.utils.spinner_utils import run_step_with_spinner

logger = logging.getLogger("dataops")
dockerize_service = DockerizeService(logger)

pipeline_id = "mock_etl_pipeline_20251225_132447_5b8a3759"
spec = {
    "pipeline_name": "mock_etl_pipeline"
}

async def main():
    step_msg = "Step 8: Dockerizing and deploying the pipeline..."
    step_number = 8
    dockerize_result, error = await run_step_with_spinner(step_msg, step_number, dockerize_service.build_and_test_docker_image,
        pipeline_id,
    )
    print(step_msg)
    print("Dockerize result:", json.dumps(dockerize_result))
    print("Error:", error)
