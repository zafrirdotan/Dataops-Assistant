# Data Loader Service

A standalone service for initializing data in the DataOps Assistant application.

## Features

- CSV file loading and processing
- Database schema initialization
- Initial data seeding
- MinIO integration support
- Environment-based configuration

## Usage

### Standalone Python Execution

```bash
cd data-loader-service
pip install -r requirements.txt
export DATABASE_HOST=localhost
export DATABASE_PORT=5432
export DATABASE_NAME=dataops
export DATABASE_USER=postgres
export DATABASE_PASSWORD=password
export CSV_DATA_PATH=/path/to/csv/files
python src/main.py
```

### Docker Execution

```bash
cd data-loader-service
docker build -t data-loader-service .
docker run --env-file .env data-loader-service
```

### Docker Compose Integration

Add this service to your main docker-compose.yml:

```yaml
services:
  data-loader:
    build: ./data-loader-service
    environment:
      - DATABASE_HOST=postgres
      - DATABASE_PORT=5432
      - DATABASE_NAME=dataops
      - DATABASE_USER=postgres
      - DATABASE_PASSWORD=password
      - CSV_DATA_PATH=/data/csv
    volumes:
      - ./data:/data
    depends_on:
      - postgres
      - minio
```

## Environment Variables

- `DATABASE_HOST`: Database host (default: localhost)
- `DATABASE_PORT`: Database port (default: 5432)
- `DATABASE_NAME`: Database name (default: dataops)
- `DATABASE_USER`: Database user (default: postgres)
- `DATABASE_PASSWORD`: Database password (default: password)
- `MINIO_HOST`: MinIO host (default: localhost)
- `MINIO_PORT`: MinIO port (default: 9000)
- `MINIO_ACCESS_KEY`: MinIO access key (default: minioadmin)
- `MINIO_SECRET_KEY`: MinIO secret key (default: minioadmin)
- `CSV_DATA_PATH`: Path to CSV files (default: /data/csv)

## Extending

To add new data loaders:

1. Create a new loader class inheriting from `BaseLoader`
2. Implement the `load_data()` method
3. Add it to the `DataLoaderManager._initialize_loaders()` method
