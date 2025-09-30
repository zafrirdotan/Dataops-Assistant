#!/usr/bin/env python3
"""
Verify what DBeaver should see in the database
"""
import os
import sys
import psycopg2
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
    
    print("🔗 DBeaver Connection Verification")
    print("=" * 50)
    
    # Show connection details
    print(f"📍 Connection Details:")
    print(f"   Host: {DB_CONFIG['host']}")
    print(f"   Port: {DB_CONFIG['port']}")
    print(f"   Database: {DB_CONFIG['database']}")
    print(f"   User: {DB_CONFIG['user']}")
    print()
    
    # List all schemas
    cursor.execute("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name")
    schemas = [row[0] for row in cursor.fetchall()]
    print(f"📁 Available Schemas: {schemas}")
    print()
    
    # List all tables in public schema
    cursor.execute("""
        SELECT table_name, table_type 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        ORDER BY table_name
    """)
    tables = cursor.fetchall()
    print(f"📊 Tables in 'public' schema:")
    for table_name, table_type in tables:
        print(f"   └── {table_name} ({table_type})")
    print()
    
    # Check bank_transactions specifically
    if any('bank_transactions' in table[0] for table in tables):
        cursor.execute("SELECT COUNT(*) FROM bank_transactions")
        count = cursor.fetchone()[0]
        print(f"✅ bank_transactions table found with {count} records")
        
        # Show sample data
        cursor.execute("SELECT * FROM bank_transactions LIMIT 3")
        sample_data = cursor.fetchall()
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'bank_transactions' ORDER BY ordinal_position")
        columns = [row[0] for row in cursor.fetchall()]
        
        print(f"📋 Sample Data (first 3 rows):")
        print(f"   Columns: {columns}")
        for i, row in enumerate(sample_data):
            print(f"   Row {i+1}: {dict(zip(columns[:5], row[:5]))}...")  # Show first 5 columns only
    else:
        print("❌ bank_transactions table NOT found")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Connection Error: {e}")
    print("\n🔧 Troubleshooting:")
    print("1. Make sure Docker containers are running: docker-compose ps")
    print("2. Check if PostgreSQL port is accessible: docker-compose logs postgres")
    print("3. Verify .env file has correct credentials")
