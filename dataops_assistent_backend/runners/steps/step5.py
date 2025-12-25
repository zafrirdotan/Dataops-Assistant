# step5.py
import json
from pipeline_builder.deployment.pipeline_output_service import PipelineOutputService
from pipeline_builder.types import CodeGenResult
from shared.utils.spinner_utils import run_step_with_spinner


mock_pipeline_code: CodeGenResult = {
    "pipeline": """# Mock pipeline code
    def etl_pipeline():
        print("This is a mock ETL pipeline.")
    """,
    "requirements": """# Mock requirements
    pandas
    numpy
    """,
    "tests": """# Mock tests
    def test_etl_pipeline():
        assert True
    """
}

spec = {
    "pipeline_name": "mock_etl_pipeline"
}


async def main(*args):
    step_msg = "Step 5 executed. Store generated pipeline files."
    step_number = 5
    output_service = PipelineOutputService()
    pipeline_info, error = await run_step_with_spinner(step_msg, step_number, output_service.store_pipeline_files,
                spec.get("pipeline_name"), 
                mock_pipeline_code, 
            )
    
    print(step_msg)
    print("Pipeline Info:", json.dumps(pipeline_info))
    print("Error:", error)
