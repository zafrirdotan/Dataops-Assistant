
# step8.py
import json
import logging
from pipeline_builder.deployment.dockerize_service import DockerizeService
from shared.utils.spinner_utils import run_step_with_spinner

logger = logging.getLogger("dataops")
dockerize_service = DockerizeService(logger)

pipeline_id = "bank_transactions_to_parquet_and_sqlite_20251230_1549_20251230_155009_4b07864b"
spec = {
    "pipeline_name": "mock_etl_pipeline"
}

async def main():
    step_msg = "Step 8: Dockerizing and deploying the pipeline..."
    step_number = 8
    dockerize_result, error = await run_step_with_spinner(step_msg, step_number, dockerize_service.dockerize_pipeline_v2,
        pipeline_id,
    )
    print(step_msg)
    print("Dockerize result:", json.dumps(dockerize_result))
    print("Error:", error)

    run_pipeline_result, error = await run_step_with_spinner("Running the dockerized pipeline...", step_number + 1,
        dockerize_service.run_pipeline_in_container,
        dockerize_result["image_id"],
    )

    print("Run pipeline result:", json.dumps(run_pipeline_result))
    print("Error:", error)
