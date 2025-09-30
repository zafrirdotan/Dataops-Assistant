#!/usr/bin/env python3
"""
Quick script to check database state
"""
import os
import sys
import psycopg2
from dotenv import load_dotenv

# Load environment variables
env_file = os.path.join(os.path.dirname(__file__), '../../.env')
if os.path.exists(env_file):
    load_dotenv(env_file)

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': os.getenv('POSTGRES_DB', 'dataops_db'),
    'user': os.getenv('POSTGRES_USER', 'dataops_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'dataops_password')
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'bank_transactions'
        );
    """)
    table_exists = cursor.fetchone()[0]
    print(f"🔍 Table 'bank_transactions' exists: {table_exists}")
    
    if table_exists:
        # Check record count
        cursor.execute("SELECT COUNT(*) FROM bank_transactions")
        count = cursor.fetchone()[0]
        print(f"📊 Current record count: {count}")
        
        # Show first few transaction IDs
        cursor.execute("SELECT transaction_id FROM bank_transactions ORDER BY id LIMIT 5")
        sample_ids = cursor.fetchall()
        print(f"📋 Sample transaction IDs: {[row[0] for row in sample_ids]}")
        
        # Show table statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT merchant) as unique_merchants,
                COUNT(DISTINCT category) as unique_categories,
                MIN(transaction_date) as earliest_date,
                MAX(transaction_date) as latest_date,
                SUM(CASE WHEN amount > 0 THEN 1 ELSE 0 END) as credits,
                SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) as debits
            FROM bank_transactions
        """)
        stats = cursor.fetchone()
        print(f"📈 Database Statistics:")
        print(f"   Total Records: {stats[0]}")
        print(f"   Unique Merchants: {stats[1]}")
        print(f"   Unique Categories: {stats[2]}")
        print(f"   Date Range: {stats[3]} to {stats[4]}")
        print(f"   Credits: {stats[5]}, Debits: {stats[6]}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Error: {e}")