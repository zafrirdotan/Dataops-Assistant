#!/usr/bin/env python3
"""
CSV to Database Loader Script
Loads bank_transactions.csv data into PostgreSQL database
"""

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import os
import sys

def load_csv_to_database():
    """Load CSV data into PostgreSQL database"""
    
    # Database connection parameters
    DB_CONFIG = {
        'host': 'localhost',
        'port': '5432',
        'database': 'dataops_db',
        'user': 'dataops_user',
        'password': 'dataops_password'
    }
    
    # File paths
    csv_file = '../../data/bank_transactions.csv'
    sql_file = '../create_bank_transactions_table.sql'
    
    print("🏦 DataOps Assistant - CSV Data Loader")
    print("=" * 50)
    
    try:
        # Step 1: Check if CSV file exists
        if not os.path.exists(csv_file):
            print(f"❌ CSV file not found: {csv_file}")
            return False
            
        print(f"✅ Found CSV file: {csv_file}")
        
        # Step 2: Load CSV data
        print("📊 Loading CSV data...")
        df = pd.read_csv(csv_file)
        print(f"   Loaded {len(df)} rows with {len(df.columns)} columns")
        print(f"   Columns: {list(df.columns)}")
        
        # Step 3: Create database connection
        print("🔗 Connecting to database...")
        connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(connection_string)
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("   ✅ Database connection successful")
        
        # Step 4: Create table (run SQL script)
        print("🔧 Creating/updating table schema...")
        if os.path.exists(sql_file):
            with open(sql_file, 'r') as f:
                sql_content = f.read()
            
            with engine.connect() as conn:
                # Split SQL commands and execute them
                sql_commands = [cmd.strip() for cmd in sql_content.split(';') if cmd.strip()]
                for cmd in sql_commands:
                    if cmd:
                        conn.execute(text(cmd))
                        conn.commit()
            print("   ✅ Table schema created/updated")
        else:
            print(f"   ⚠️  SQL file not found: {sql_file}, skipping schema creation")
        
        # Step 5: Clear existing data (optional)
        print("🗑️  Clearing existing data...")
        with engine.connect() as conn:
            result = conn.execute(text("DELETE FROM public.bank_transactions"))
            deleted_rows = result.rowcount
            conn.commit()
            print(f"   Deleted {deleted_rows} existing rows")
        
        # Step 6: Load data to database
        print("💾 Loading data to database...")
        
        # Convert date and time columns to proper format
        df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.date
        df['transaction_time'] = pd.to_datetime(df['transaction_time'], format='%H:%M:%S').dt.time
        
        # Handle empty notes column
        df['notes'] = df['notes'].fillna('')
        
        # Load data using pandas to_sql
        df.to_sql(
            'bank_transactions',
            engine,
            schema='public',
            if_exists='append',  # append since we cleared existing data
            index=False,
            method='multi'  # faster bulk insert
        )
        
        print(f"   ✅ Successfully loaded {len(df)} rows to bank_transactions table")
        
        # Step 7: Verify data was loaded
        print("🔍 Verifying data load...")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM public.bank_transactions"))
            count = result.fetchone()[0]
            print(f"   Database contains {count} rows")
            
            # Show sample data
            result = conn.execute(text("SELECT * FROM public.bank_transactions LIMIT 3"))
            sample_rows = result.fetchall()
            print("   Sample data:")
            for i, row in enumerate(sample_rows, 1):
                print(f"     Row {i}: {row[0]} | {row[1]} | {row[6]} | {row[7]} | {row[8]}")
        
        print("\n🎉 Data loading completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Error loading data: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = load_csv_to_database()
    sys.exit(0 if success else 1)
