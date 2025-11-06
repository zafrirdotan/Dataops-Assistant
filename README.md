# MLOps Final Project – DataOps Assistant: SQL + ETL Co-Pilot

## Dataops-Assistant

### Project Summary

DataOps Assistant is an intelligent co-pilot platform for automating, orchestrating, and managing data engineering workflows, including SQL analytics and ETL (Extract, Transform, Load) pipelines. It leverages LLM-powered code generation and validation to streamline the creation, testing, and deployment of data pipelines, making modern data operations accessible and efficient for both data engineers and analysts.

### Main Capabilities

- **Automated Pipeline Generation:** Generate ETL pipeline code and SQL queries from natural language specifications using LLMs.
- **Schema and Data Validation:** Validate pipeline specs and data schemas before execution to ensure data quality and compliance.
- **End-to-End Orchestration:** Build, test, and deploy pipelines with integrated Docker, Airflow, and MinIO support.
- **Database Integration:** Seamless support for PostgreSQL (primary), SQLite (for isolated tests), and Parquet file outputs.
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

1. **Clone the project**

   ```bash
   git clone <repo-url>
   cd Dataops-Assistant
   ```

2. **Run setup script**

   ```bash
   ./setup.sh
   # or
   bash setup.sh
   ```

3. **Build and run Airflow compose**

   ```bash
   docker compose -f docker-compose.airflow.yml up -d --build
   ```

4. **Build and run main compose**
   ```bash
   docker compose -f docker-compose.yml up -d --build
   ```

### Database Checks

- Verify schemas exist (`dw`, `public`, `dataops_assistant`)
- Verify tables in public schema (`bank_transactions`, `transaction`, `customers`)

### Data Files

**Input:**

- Data Folder: Place CSV files in the project's root `data/` directory (`./data/<your-file>.csv`)
- DB: PostgreSQL (local, running in Docker)

_At this point, it is the same DB for input, output, and the system DB._

**Output:**

- The output Parquet and SQLite files will be located in the output folder after a manual or scheduled run.
- _Note: The output folder is currently used for both production and testing; this will be resolved in the future._
