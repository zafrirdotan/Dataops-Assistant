# step9.py
import json
import logging
from pipeline_builder.deployment.scheduler_service import SchedulerService
from shared.utils.spinner_utils import run_step_with_spinner
logger = logging.getLogger("dataops")
scheduler_service = SchedulerService(logger)

pipeline_id = "mock_etl_pipeline_20251225_132447_5b8a3759"
spec = {
    "pipeline_name": "mock_etl_pipeline"
}
async def main():
    step_msg = "Step 9: Scheduling the pipeline..."
    step_number = 9
    scheduled_result, error = await run_step_with_spinner(step_msg, step_number, scheduler_service.save_pipeline_to_catalog,
        pipeline_id,
        spec
    )
    print(step_msg)
    print("Scheduling result:", json.dumps(scheduled_result))
    print("Error:", error)
