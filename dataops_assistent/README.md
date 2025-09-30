# dataops_assistent

A minimal FastAPI application with Docker support.

## Usage

### Local Run

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Start the app:
   ```bash
   uvicorn app.main:app --reload
   ```

### Docker Run

1. Build the Docker image:
   ```bash
   docker build -t dataops-assistent .
   ```
2. Run the container:
   ```bash
   docker run -p 80:80 dataops-assistent
   ```

The app will be available at http://localhost/
