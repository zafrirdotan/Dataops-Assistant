# step7.py
import json
from pipeline_builder.registry.pipeline_registry_service import getPipelineRegistryService
from shared.utils.spinner_utils import run_step_with_spinner

pipeline_registry = getPipelineRegistryService()

pipeline_id = "mock_etl_pipeline_20251225_132447_5b8a3759"
pipeline_name = "mock_etl_pipeline"
spec = {
    "pipeline_name": "mock_etl_pipeline"
}
async def main():
    step_msg = "Step 7: Registering pipeline in the registry..."
    step_number = 7
    _, error = await run_step_with_spinner(step_msg, step_number, pipeline_registry.create_pipeline,
        pipeline_id=pipeline_id,
        name=pipeline_name,
        created_by="some_user",
        description="A mock ETL pipeline for demonstration purposes.",
        spec=spec
    )
    print(step_msg)
    print("Is Registry Success:", error is None)
    print("Registry Error:", error)

