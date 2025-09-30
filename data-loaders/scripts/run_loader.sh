#!/bin/bash
# Simple script to run the data loader tools

echo "🏦 DataOps Assistant - Data Loader Tools"
echo "========================================"
echo ""

# Check if we're in the right directory
if [ ! -f "check_db_state.py" ]; then
    echo "❌ Please run this script from the data-loaders/scripts directory"
    echo "Usage: cd data-loaders/scripts && ./run_loader.sh"
    exit 1
fi

echo "1. 🔍 Checking database state..."
python3 check_db_state.py
echo ""

echo "2. 🔗 Verifying DBeaver connection..."
python3 verify_dbeaver_connection.py
echo ""

echo "✅ All checks completed!"
echo ""
echo "📋 To connect with DBeaver:"
echo "   Host: localhost"
echo "   Port: 5432"
echo "   Database: dataops_db"
echo "   Username: dataops_user"
echo "   Password: dataops_password"
