
# step4.py
import json
import logging
from pipeline_builder.generators.pipeline_code_generator_LLM_hybrid import PipelineCodeGeneratorLLMHybrid
from shared.utils.spinner_utils import run_step_with_spinner

logger = logging.getLogger("dataops")

code_gen = PipelineCodeGeneratorLLMHybrid(logger)
db_info = {
    "success": True,
    "data_preview": [
        {"customer_id": 1.0, "first_name": "Robert", "last_name": "Butler", "email": "qlee@may.org", "country": "Kazakhstan", "signup_date": "2025-01-31", "is_active": True, "transaction_id": None, "user_id": None, "account_id": None, "transaction_date": None, "transaction_time": None, "amount": None, "currency": None, "merchant": None, "category": None, "transaction_type": None, "status": None, "location": None, "device": None, "balance_after": None, "notes": None, "txn_id": None},
        {"customer_id": 2.0, "first_name": "Mark", "last_name": "Villa", "email": "vasquezlori@henry-miranda.com", "country": "Reunion", "signup_date": "2025-01-05", "is_active": False, "transaction_id": None, "user_id": None, "account_id": None, "transaction_date": None, "transaction_time": None, "amount": None, "currency": None, "merchant": None, "category": None, "transaction_type": None, "status": None, "location": None, "device": None, "balance_after": None, "notes": None, "txn_id": None},
        {"customer_id": 3.0, "first_name": "Rachel", "last_name": "Mckinney", "email": "hblake@yahoo.com", "country": "Belize", "signup_date": "2024-02-15", "is_active": False, "transaction_id": None, "user_id": None, "account_id": None, "transaction_date": None, "transaction_time": None, "amount": None, "currency": None, "merchant": None, "category": None, "transaction_type": None, "status": None, "location": None, "device": None, "balance_after": None, "notes": None, "txn_id": None},
        {"customer_id": 4.0, "first_name": "Charles", "last_name": "Jones", "email": "nicole74@hotmail.com", "country": "Canada", "signup_date": "2025-09-25", "is_active": True, "transaction_id": None, "user_id": None, "account_id": None, "transaction_date": None, "transaction_time": None, "amount": None, "currency": None, "merchant": None, "category": None, "transaction_type": None, "status": None, "location": None, "device": None, "balance_after": None, "notes": None, "txn_id": None},
        {"customer_id": 5.0, "first_name": "Paul", "last_name": "James", "email": "anthonymartin@gmail.com", "country": "El Salvador", "signup_date": "2024-04-24", "is_active": True, "transaction_id": None, "user_id": None, "account_id": None, "transaction_date": None, "transaction_time": None, "amount": None, "currency": None, "merchant": None, "category": None, "transaction_type": None, "status": None, "location": None, "device": None, "balance_after": None, "notes": None, "txn_id": None}
    ]
}
spec = {"pipeline_name": "ingest_csv_to_parquet_and_sqlite_20251225_1255", "description": "Ingest CSV files from the data directory into a Parquet file partitioned by date and a SQLite table named orders_daily, running daily at 02:00.", "source_type": "localFileCSV", "source_table": "", "source_path": "./data/*.csv", "destination_type": "sqlite", "destination_name": "orders_daily", "transformation_logic": "", "schedule": "0 2 * * *"}

async def main():
    step_msg = "Step 4: Generating pipeline code..."
    step_number = 4
    pipeline_code, error = await run_step_with_spinner(step_msg, step_number, code_gen.generate_code, spec, db_info)
    print(step_msg)
    print("\n========== PIPELINE ==========")
    print(pipeline_code.get("pipeline", "<no pipeline code>").strip())
    print("========== END PIPELINE ==========")

    print("\n========== REQUIREMENTS ==========")
    print(pipeline_code.get("requirements", "<no requirements>").strip())
    print("========== END REQUIREMENTS ==========")

    print("\n========== TESTS ==========")
    print(pipeline_code.get("tests", "<no tests>").strip())
    print("========== END TESTS ==========")

    print("Error:", error)
