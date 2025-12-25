
# step1.py
import json
import logging
from pipeline_builder.generators.pipeline_spec_generator import PipelineSpecGenerator
from shared.utils.spinner_utils import run_step_with_spinner

logger = logging.getLogger("dataops")
spec_gen = PipelineSpecGenerator(logger)

async def main():
    user_input = "Ingest ./data/*.csv to Parquet (partition by date) and SQLite table orders_daily, run daily at 02:00."
    step_msg = "Step 1: Generating pipeline specification..."
    step_number = 1
    spec, error = await run_step_with_spinner(step_msg, step_number, spec_gen.generate_spec, user_input)
    print(step_msg)
    print("Spec:", json.dumps(spec))
    print("Error:", error)
