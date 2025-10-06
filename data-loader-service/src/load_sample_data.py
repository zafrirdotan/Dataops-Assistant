#!/usr/bin/env python3
"""
Script to load sample bank transactions data into PostgreSQL database.
Run this after the database is initialized to populate the transactions table.
"""

import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_sample_data():
    """Load sample bank transactions data into PostgreSQL."""
    
    # Database connection - updated for data-loader-service environment
    DATABASE_URL = os.getenv("DATABASE_URL", 
                            f"postgresql://{os.getenv('DATABASE_USER', 'dataops_user')}:"
                            f"{os.getenv('DATABASE_PASSWORD', 'dataops_password')}@"
                            f"{os.getenv('DATABASE_HOST', 'localhost')}:"
                            f"{os.getenv('DATABASE_PORT', '5432')}/"
                            f"{os.getenv('DATABASE_NAME', 'dataops_db')}")
    
    try:
        # Create SQLAlchemy engine
        engine = create_engine(DATABASE_URL)
        
        # Read CSV data - updated paths for data-loader-service
        csv_data_dir = os.getenv('CSV_DATA_PATH', '/data')
        csv_path = os.path.join(csv_data_dir, 'bank_transactions.csv')
        
        if not os.path.exists(csv_path):
            # Try alternative paths
            alternative_paths = [
                "/data/bank_transactions.csv",
                "../data/bank_transactions.csv",
                "../../data/bank_transactions.csv",
                "/app/data/bank_transactions.csv"
            ]
            for alt_path in alternative_paths:
                if os.path.exists(alt_path):
                    csv_path = alt_path
                    break
            else:
                raise FileNotFoundError(f"Could not find bank_transactions.csv in any expected location. Searched: {alternative_paths}")
        
        logger.info(f"Reading CSV data from: {csv_path}")
        df = pd.read_csv(csv_path)
        
        # Convert date columns
        df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.date
        df['transaction_time'] = pd.to_datetime(df['transaction_time'], format='%H:%M:%S').dt.time
        
        # Load data into PostgreSQL
        logger.info(f"Loading {len(df)} transactions into database...")
        df.to_sql('transactions', engine, schema='public', if_exists='replace', index=False)
        
        logger.info("Sample data loaded successfully!")
        
        # Verify the data was loaded
        from sqlalchemy import text
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM public.transactions"))
            count = result.fetchone()[0]
            logger.info(f"Verified: {count} transactions in database")
            
            # Show sample records
            result = conn.execute(text("SELECT * FROM public.transactions LIMIT 5"))
            sample_data = result.fetchall()
            logger.info("Sample records:")
            for row in sample_data:
                logger.info(f"  {row}")
        
    except Exception as e:
        logger.error(f"Error loading sample data: {e}")
        raise

if __name__ == "__main__":
    load_sample_data()
