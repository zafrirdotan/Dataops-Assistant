
# step6.py
import json
import logging
from pipeline_builder.testing.pipeline_test_service import PipelineTestService
from shared.utils.spinner_utils import run_step_with_spinner

logging = logging.getLogger("dataops")

test_service = PipelineTestService(logging)


async def main():
    step_msg = "Step 6: Running pipeline tests from MinIO storage..."
    step_number = 6
    test_result, error = await run_step_with_spinner(step_msg, step_number, test_service.run_pipeline_test_in_venv_v2,
        "mock_etl_pipeline_20260127_083830_8c9f66b4"
    )
    print(step_msg)
    print("Test result:", json.dumps(test_result))
    print("Error:", error)
