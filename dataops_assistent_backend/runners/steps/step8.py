
# step8.py
import json
import logging
from pipeline_builder.deployment.dockerize_service import DockerizeService
from shared.utils.spinner_utils import run_step_with_spinner

logger = logging.getLogger("dataops")
dockerize_service = DockerizeService(logger)

pipeline_id = "bank_transactions_to_parquet_and_sqlite_20251228_1320_20251228_132029_1920b598"
spec = {
    "pipeline_name": "mock_etl_pipeline"
}

async def main():
    step_msg = "Step 8: Dockerizing and deploying the pipeline..."
    step_number = 8
    dockerize_result, error = await run_step_with_spinner(step_msg, step_number, dockerize_service.test_pipeline_in_docker,
        pipeline_id,
    )
    print(step_msg)
    print("Dockerize result:", json.dumps(dockerize_result))
    print("Error:", error)
