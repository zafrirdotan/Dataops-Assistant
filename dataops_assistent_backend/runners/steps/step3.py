
# step3.py
import json
import logging
from pipeline_builder.sources.source_service import SourceService
from shared.utils.spinner_utils import run_step_with_spinner

logger = logging.getLogger("dataops")
source_service = SourceService(logger)
spec = {"pipeline_name": "ingest_csv_to_parquet_and_sqlite_20251225_1255", "description": "Ingest CSV files from the data directory into a Parquet file partitioned by date and a SQLite table named orders_daily, running daily at 02:00.", "source_type": "localFileCSV", "source_table": "", "source_path": "./data/*.csv", "destination_type": "sqlite", "destination_name": "orders_daily", "transformation_logic": "", "schedule": "0 2 * * *"}

async def main():
    step_msg = "Step 3: Connecting to source/destination to validate access..."
    step_number = 3
    db_info, error = await run_step_with_spinner(step_msg, step_number, source_service.fetch_data_from_source, spec, limit=5)
    print(step_msg)
    print("DB Info:", json.dumps(db_info))
    print("Error:", error)
