import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, Mock
import sys
import os

# Add the app directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


@pytest.mark.integration
class TestIntegration:
    """Integration tests for the API endpoints."""

    # @patch('app.services.storage_service.MinioStorage')
    # @patch('app.services.chat_service.PipelineBuilderService')
    # @patch('app.services.chat_service.PromptGuardService')
    # @patch('app.services.chat_service.LLMService')
    # def test_end_to_end_chat_flow(self, mock_llm, mock_guard, mock_pipeline, mock_minio):
    #     """Test complete chat flow from API to response."""
    #     from app.main import app
        
    #     # Arrange
    #     mock_guard_instance = Mock()
    #     mock_pipeline_instance = Mock()
    #     mock_guard.return_value = mock_guard_instance
    #     mock_pipeline.return_value = mock_pipeline_instance
        
    #     mock_guard_instance.analyze.return_value = {
    #         "decision": "allow",
    #         "cleaned": "create a data pipeline",
    #         "findings": []
    #     }
    #     mock_pipeline_instance.build_pipeline.return_value = {
    #         "success": True,
    #         "spec": {"pipeline_name": "test_pipeline"},
    #         "code": "# Generated pipeline code"
    #     }
        
    #     client = TestClient(app)
        
    #     # Act
    #     response = client.post("/chat", json={
    #         "message": "Create a data pipeline to process CSV files"
    #     })
        
    #     # Assert
    #     assert response.status_code == 200
    #     data = response.json()
    #     assert "response" in data
    #     assert data["response"]["success"] is True

    @patch('app.services.storage_service.MinioStorage')
    def test_health_check_integration(self, mock_minio):
        """Test health check endpoint integration."""
        from app.main import app
        
        client = TestClient(app)
        response = client.get("/health")
        
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
