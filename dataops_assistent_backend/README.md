# DataOps Assistant Backend

This is the backend service for the DataOps Assistant project. It provides a FastAPI-based REST API for managing data pipelines, integrating with PostgreSQL for storage and MinIO for object storage.

## Key Features

- FastAPI web API for pipeline management and chat operations
- PostgreSQL integration for pipeline metadata and execution tracking
- MinIO integration for storing pipeline artifacts and data files
- Automated test suite using pytest
- Docker Compose support for local development

## Getting Started

### With Docker Compose

1. Build and start all services:
   ```bash
   docker compose up -d --build
   ```
2. The API will be available at [http://localhost:8080](http://localhost:8080)
3. MinIO console: [http://localhost:9001](http://localhost:9001) (user: minioadmin, pass: minioadmin)

### Local Development

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Copy and edit environment variables:
   ```bash
   cp .env.example .env
   # Edit .env as needed
   ```
3. Start the app:
   ```bash
   uvicorn app.main:app --reload
   ```

## API Usage

Key endpoints:

- `POST /chat` — Chat with the assistant
- `POST /trigger-pipeline?pipeline_id={pipeline_id}` — Trigger a pipeline run
- `GET /pipelines` — List all pipelines
- API docs: [http://localhost:8080/docs](http://localhost:8080/docs)

## Configuration

Set environment variables in your `.env` file. Example:

```env
DATABASE_URL=postgresql://dataops_user:dataops_password@postgres:5432/dataops_db
S3_ENDPOINT=http://minio:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_BUCKET=dataops-bucket
OPENAI_API_KEY=your_openai_api_key
```

## Testing

Run all tests:

```bash
pytest
```

## Project Structure

```
app/
  main.py
  routes/
  services/
  models/
  utils/
tests/
```

## Contributing

1. Add new service logic in `app/services/`
2. Add or update API routes in `app/routes/`
3. Write or update tests in `tests/`
4. Update database schema in `init.sql` if needed

---

For more details, see the main project README.
