# Data Loaders

This directory contains scripts and configurations for loading data into the DataOps Assistant PostgreSQL database.

## 📁 Directory Structure

```
data-loaders/
├── scripts/                    # Executable scripts
│   ├── load_bank_data.py      # Main bank data loader (Python)
│   ├── load_bank_data.sh      # Shell wrapper script
│   ├── check_db_state.py      # Database state checker
│   └── verify_dbeaver_connection.py  # DBeaver connection helper
├── config/                     # Configuration files
│   └── database.yml           # Database configuration
├── requirements/               # Python dependencies
│   └── requirements.txt       # Required packages
├── logs/                      # Log files (created automatically)
└── docs/                      # Documentation
    └── README.md              # This file
```

## 🚀 Quick Start

### Option 1: Using Shell Script (Recommended)

```bash
cd data-loaders/scripts
./load_bank_data.sh
```

### Option 2: Using Python Directly

```bash
cd data-loaders/scripts
pip3 install -r ../requirements/requirements.txt
python3 load_bank_data.py --csv-file ../../data/bank_transactions.csv
```

## 📊 Available Scripts

### 1. Bank Data Loader (`load_bank_data.py`)

**Purpose:** Load bank transaction CSV data into PostgreSQL with validation and error handling.

**Features:**

- ✅ Data validation and cleaning
- 🔄 Batch processing for large files
- 🚫 Duplicate handling (skip or update)
- 📈 Progress tracking and statistics
- 🔄 Error recovery and logging
- 🗃️ Automatic table creation with indexes

**Usage:**

```bash
# Test connection
python3 load_bank_data.py --test-connection

# Load data (skip duplicates)
python3 load_bank_data.py --csv-file ../../data/bank_transactions.csv

# Update existing records
python3 load_bank_data.py --csv-file ../../data/bank_transactions.csv --update-duplicates

# Drop and recreate table
python3 load_bank_data.py --csv-file ../../data/bank_transactions.csv --drop-table

# Custom batch size
python3 load_bank_data.py --csv-file ../../data/bank_transactions.csv --batch-size 500
```

### 2. Shell Wrapper (`load_bank_data.sh`)

**Purpose:** Automated script with environment checks and setup.

**What it does:**

- 🐳 Checks Docker status
- 🗄️ Starts PostgreSQL container if needed
- 📦 Installs Python dependencies
- 🔌 Tests database connection
- 📊 Loads data with progress feedback

### 3. Database State Checker (`check_db_state.py`)

**Purpose:** Quick verification of database contents.

**Shows:**

- 📊 Record counts
- 📋 Sample data
- 📈 Database statistics
- 🔍 Duplicate detection

### 4. DBeaver Connection Helper (`verify_dbeaver_connection.py`)

**Purpose:** Troubleshoot DBeaver connection issues.

**Provides:**

- 🔗 Connection verification
- 📁 Schema and table listing
- 📊 Sample data preview
- 🔧 Troubleshooting tips

## ⚙️ Configuration

### Environment Variables

The scripts use these environment variables from `../../.env`:

```
POSTGRES_DB=dataops_db
POSTGRES_USER=dataops_user
POSTGRES_PASSWORD=dataops_password
DB_HOST=localhost
DB_PORT=5432
```

### Database Configuration (`config/database.yml`)

- Connection settings
- Pool configuration
- Loading parameters
- Table schemas and constraints

## 📋 Database Schema

The `bank_transactions` table is created with:

```sql
CREATE TABLE bank_transactions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) UNIQUE NOT NULL,
    user_id INTEGER,
    account_id INTEGER,
    transaction_date DATE,
    transaction_time TIME,
    amount DECIMAL(12,2),
    currency VARCHAR(3),
    merchant VARCHAR(100),
    category VARCHAR(50),
    transaction_type VARCHAR(20),
    status VARCHAR(20),
    location VARCHAR(100),
    device VARCHAR(20),
    balance_after DECIMAL(12,2),
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

**Indexes created for performance:**

- `user_id`
- `account_id`
- `transaction_date`
- `merchant`
- `category`
- `status`

## 🔧 Troubleshooting

### Common Issues

1. **"Connection refused" Error**

   ```bash
   # Check if PostgreSQL is running
   docker-compose ps postgres

   # Start if needed
   docker-compose up -d postgres
   ```

2. **"Permission denied" Error**

   ```bash
   # Make shell script executable
   chmod +x load_bank_data.sh
   ```

3. **"Module not found" Error**

   ```bash
   # Install dependencies
   pip3 install -r requirements/requirements.txt
   ```

4. **"All records skipped" Message**
   - Data already exists (normal behavior)
   - Use `--update-duplicates` to update existing records
   - Use `--drop-table` to start fresh

### Logs

- Log files are created in `logs/data_loader.log`
- Includes timestamps, error details, and statistics
- Rotated automatically when they get large

## 📈 Performance Tips

1. **Batch Size:** Adjust `--batch-size` based on memory (default: 1000)
2. **Large Files:** Use smaller batch sizes for very large CSV files
3. **Network:** Run on same machine as PostgreSQL for best performance
4. **Indexes:** Indexes are created automatically for query optimization

## 🔒 Security Notes

- Database credentials are loaded from `.env` file
- Never commit `.env` file to version control
- Use strong passwords in production
- Consider using connection pooling for high-volume loads

## 📞 Support

If you encounter issues:

1. Check the logs in `logs/data_loader.log`
2. Run the database state checker: `python3 check_db_state.py`
3. Verify DBeaver connection: `python3 verify_dbeaver_connection.py`
4. Check Docker containers: `docker-compose ps`
