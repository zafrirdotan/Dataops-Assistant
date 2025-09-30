# DataOps Assistant Backend

A FastAPI application with PostgreSQL and MinIO integration for data operations and pipeline management.

## Features

- 🚀 FastAPI web framework with async support
- 🐘 PostgreSQL database integration with SQLAlchemy
- 📦 MinIO object storage for data files
- 🧪 Comprehensive test suite with pytest
- 🐳 Docker containerization with docker-compose
- 📊 Data pipeline generation and management
- 💬 Chat interface for data operations

## Architecture

- **API Layer**: FastAPI routes and endpoints
- **Service Layer**: Business logic and data processing
- **Database Layer**: PostgreSQL with SQLAlchemy ORM
- **Storage Layer**: MinIO for object storage
- **Pipeline Layer**: Dynamic data pipeline generation

## Usage

### Docker Compose (Recommended)

1. Start all services:
   ```bash
   docker-compose up -d
   ```

This will start:

- DataOps Assistant API (port 8080)
- PostgreSQL database (port 5432)
- MinIO object storage (port 9000, console: 9001)

2. Check service status:
   ```bash
   curl http://localhost:8080/health
   ```

### Local Development

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Set up environment variables:

   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. Start the app:
   ```bash
   uvicorn app.main:app --reload
   ```

## Database

The application uses PostgreSQL with the following tables:

- `pipelines`: Store data pipeline metadata and code
- `pipeline_executions`: Track pipeline execution history
- `chat_history`: Store chat conversation history

### Database Schema

```sql
-- Pipelines table
CREATE TABLE pipelines (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    spec JSONB,
    code TEXT,
    status VARCHAR(50) DEFAULT 'created',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

## API Endpoints

### Core Endpoints

- `GET /` - API information
- `GET /health` - Health check with component status
- `POST /chat` - Chat interface for data operations

### Database Endpoints

- `GET /database/test-connection` - Test database connectivity
- `GET /database/pipelines` - List all pipelines
- `GET /database/pipelines/{id}` - Get specific pipeline
- `GET /database/chat-history` - Get chat history

## Configuration

Key environment variables:

```bash
# Database
DATABASE_URL=postgresql://dataops_user:dataops_password@postgres:5432/dataops_db
POSTGRES_DB=dataops_db
POSTGRES_USER=dataops_user
POSTGRES_PASSWORD=dataops_password

# MinIO Storage
S3_ENDPOINT=http://minio:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_BUCKET=dataops-bucket

# OpenAI (for chat functionality)
OPENAI_API_KEY=your_openai_api_key
```

## Testing

Run the test suite:

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=app

# Run specific test file
pytest tests/services/test_database_service.py
```

## Development

### Project Structure

```
app/
├── main.py              # FastAPI application entry point
├── routes/              # API route handlers
│   ├── chat.py         # Chat endpoints
│   ├── data.py         # Data management endpoints
│   └── database.py     # Database endpoints
└── services/           # Business logic services
    ├── chat_service.py       # Chat processing
    ├── database_service.py   # Database operations
    ├── llm_service.py        # LLM integration
    ├── pipeline_builder_service.py  # Pipeline generation
    └── storage_service.py    # MinIO integration
```

### Adding New Features

1. Create service classes in `app/services/`
2. Add API routes in `app/routes/`
3. Write tests in `tests/`
4. Update database schema in `init.sql` if needed

The app will be available at http://localhost:8080/
