#!/usr/bin/env python3
"""
Test script to verify the pipeline builder service can connect to PostgreSQL
and query the transactions table properly.
"""

import sys
import os

# Add the app directory to the Python path
sys.path.append('/Users/zafrirdotan/Documents/dev/ml-ops/Projects/Dataops-Assistant/dataops_assistent_backend')

from app.services.pipeline import PipelineBuilderService
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_postgres_connection():
    """Test PostgreSQL connection and data preview."""
    
    # Create a sample spec for testing
    test_spec = {
        "pipeline_name": "test_pipeline",
        "source_type": "PostgreSQL",
        "source_table": "transactions",  # or "public.transactions"
        "destination_type": "file",
        "destination_name": "output_transactions",
        "transformation": "SELECT * FROM transactions WHERE amount > 1000",
        "schedule": "0 9 * * *"
    }
    
    # Initialize the pipeline builder service
    service = PipelineBuilderService()
    
    # Test the connection
    print("Testing PostgreSQL connection...")
    result = service.connect_to_source(test_spec)
    
    print(f"Connection result: {result}")
    
    if result.get("success"):
        print("✅ PostgreSQL connection successful!")
        if result.get("data_preview"):
            print(f"✅ Data preview retrieved: {len(result['data_preview'])} rows")
            print("Sample data:")
            for i, row in enumerate(result['data_preview'][:2]):  # Show first 2 rows
                print(f"  Row {i+1}: {row}")
        else:
            print("⚠️  No data preview available")
    else:
        print(f"❌ PostgreSQL connection failed: {result}")

if __name__ == "__main__":
    test_postgres_connection()
