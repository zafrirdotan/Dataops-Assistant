# Data Loaders - Quick Reference

## 🚀 Quick Start

### Load Bank Transaction Data

```bash
cd data-loaders/scripts
./load_bank_data.sh
```

### Check Database Status

```bash
cd data-loaders/scripts
python3 check_db_state.py
```

### Troubleshoot DBeaver Connection

```bash
cd data-loaders/scripts
python3 verify_dbeaver_connection.py
```

## 📁 Directory Structure

```
data-loaders/
├── scripts/           # All executable scripts
├── config/           # Configuration files
├── requirements/     # Python dependencies
├── logs/            # Auto-generated logs
└── docs/            # Complete documentation
```

## 🔗 Related Files

- **Database API**: `dataops_assistent_backend/app/routes/database.py`
- **Environment Config**: `.env`
- **Docker Setup**: `docker-compose.yml`
- **Source Data**: `data/bank_transactions.csv`

## 📖 Full Documentation

See `data-loaders/docs/README.md` for complete documentation.
