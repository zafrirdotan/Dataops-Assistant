
from shared.utils.spinner_utils import run_step_with_spinner
from pipeline_builder.pipeline_builder_service import PipelineBuilderService

spec_gen = PipelineBuilderService()
spec = {"pipeline_name": "ingest_csv_to_parquet_and_sqlite_20251225_1255", "description": "Ingest CSV files from the data directory into a Parquet file partitioned by date and a SQLite table named orders_daily, running daily at 02:00.", "source_type": "localFileCSV", "source_table": "", "source_path": "./data/*.csv", "destination_type": "sqlite", "destination_name": "orders_daily", "transformation_logic": "", "schedule": "0 2 * * *"}
async def main():
    step_msg = "Step 2: Validating pipeline specification schema..."
    step_number = 2
    valid, error = await run_step_with_spinner(step_msg, step_number, spec_gen.validate_spec_schema, spec)
    print(step_msg)
    print("Valid:", valid)
    print("Error:", error)
