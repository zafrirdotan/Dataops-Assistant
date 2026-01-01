# MLOps Final Project – DataOps Assistant: SQL + ETL Co-Pilot

## Dataops-Assistant

### Project Summary

DataOps Assistant is an intelligent co-pilot platform for automating, orchestrating, and managing ETL (Extract, Transform, Load) pipelines. It leverages LLM-powered code generation and validation to streamline the creation, testing, and scheduling of ETL pipelines, making modern data operations accessible and efficient for both data engineers and analysts.

### 🎥 Demo

https://github.com/user-attachments/assets/5be8d792-2363-4281-9836-bcb4c059afcb

**Watch a complete walkthrough of creating and deploying an ETL pipeline with DataOps Assistant**

### Main Capabilities

- **Automated Pipeline Generation:** Generate ETL pipeline code and SQL queries from natural language specifications using LLMs.
- **Schema and Data Validation:** Validate pipeline specs and data schemas before execution to ensure data quality and compliance.
- **End-to-End Orchestration:** Build, test, and deploy pipelines with integrated Docker, Airflow, and MinIO support.
- **Database & Data Integration:** Supports local CSV files and PostgreSQL as input sources; outputs to PostgreSQL, SQLite, or Parquet files.
- **Test Automation:** Auto-generate and execute tests for pipelines, including integration with pytest and custom test runners.
- **Cloud-Native Storage:** Store pipeline artifacts and outputs in MinIO (S3-compatible object storage).
- **Scheduling & Monitoring:** Schedule pipelines via Airflow and monitor execution status and logs.

### Core Concept

DataOps Assistant bridges the gap between data engineering and MLOps by providing a unified, automated environment for building, validating, and running data pipelines. It combines LLM-driven code generation with robust validation, containerization, and orchestration, enabling rapid iteration and reliable deployment of data workflows.

### Technologies Used

- **Python** (FastAPI, pandas, SQLAlchemy, pytest)
- **Docker & Docker Compose** (for service orchestration)
- **PostgreSQL** (main database)
- **MinIO** (object storage)
- **Apache Airflow** (pipeline scheduling)
- **LLM Service** (OpenAI or compatible for code generation)
- **Pytest** (test automation)
- **Parquet, SQLite** (data output formats)
- **Bash/Shell scripting** (setup and automation)

### Quick Start

**Prerequisites:** Docker and Docker Compose installed on your system.

1. **Clone the project**

   ```bash
   git clone https://github.com/zafrirdotan/Dataops-Assistant.git
   cd Dataops-Assistant
   ```

2. **Run setup script**

   ```bash
   bash setup.sh
   ```

3. **Configure environment variables**

   - Copy `.env.example` to `.env`:
     ```bash
     cp .env.example .env
     ```
   - Update the OpenAI API key in `.env`
   - Update the absolute paths for input and output volumes

4. **Build and run Airflow compose**

   ```bash
   docker compose -f docker-compose.airflow.yml up -d --build
   ```

5. **Build and run main compose**
   ```bash
   docker compose -f docker-compose.yml up -d --build
   ```

### Access Services

Once all services are running, you can access:

- **API:** http://localhost:8080
- **MinIO Console:** http://localhost:9001 (username: `minioadmin`, password: `minioadmin`)
- **Airflow:** http://localhost:8082

### Database Checks

- Verify schemas exist (`dw`, `public`, `dataops_assistant`)
- Verify tables in public schema (`bank_transactions`, `transaction`, `customers`)

### API

The DataOps Assistant provides a REST API for interacting with pipelines and the assistant service:

- **Chat with Assistant:**  
   `POST http://localhost:8080/chat`
- **Trigger a Pipeline:**  
   `POST http://localhost:8080/trigger-pipeline?pipeline_id={pipeline_id}`
- **List All Pipelines:**  
   `GET http://localhost:8080/pipelines`
- **API Documentation:**  
   [DataOps Assistant API Docs](http://localhost:8080/docs)

Refer to the API docs for details on all available endpoints and request/response formats.

### Data Files

**Input:**

- Data Folder: Place CSV files in the project's root `data/` directory (`./data/<your-file>.csv`)
- DB: PostgreSQL (local, running in Docker) in port 5432.

_At this point, it is the same DB for input, output, and the system DB._

**Output:**

- Parquet and SQLite files will be located in the output folder after a manual or scheduled run.
- PostgreSQL data will be in DB in port 5432
- Pipelines including full Python code can be found locally in `pipelines/` directory or in MinIO at http://localhost:9001 (username: `minioadmin`, password: `minioadmin`)
- Airflow schedules can be found at http://localhost:8082/dags and in `airflow/dags/pipelines/catalog.json`
