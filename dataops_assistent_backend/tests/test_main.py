import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch
import sys
import os

# Add the app directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


@pytest.mark.unit
class TestMainApp:
    """Test cases for the main FastAPI application."""

    @patch('app.main.MinioStorage')
    def test_app_initialization(self, mock_minio):
        """Test that the app initializes correctly."""
        from app.main import app
        assert app.title == "DataOps Assistant API"
        assert app.version == "1.0.0"

    def test_root_endpoint(self, client):
        """Test the root endpoint."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "DataOps Assistant API - Ready!"
        assert data["version"] == "1.0.0"
        assert "features" in data

    def test_health_check_endpoint(self, client):
        """Test the health check endpoint."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "dataops-assistant"

    # @patch('app.main.logger')
    # @patch('app.main.MinioStorage')
    # def test_startup_with_minio_error(self, mock_minio, mock_logger):
    #     """Test app startup when MinIO initialization fails."""
    #     mock_minio.side_effect = Exception("MinIO connection failed")
        
    #     # Import after mocking to trigger the exception
    #     import importlib
    #     import app.main
    #     importlib.reload(app.main)
        
    #     mock_logger.error.assert_called_with("Error initializing storage service: MinIO connection failed")
