"""
Test cases for database routes.
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, Mock
from app.main import app


class TestDatabaseRoutes:
    """Test cases for database API routes."""
    
    def setup_method(self):
        """Set up test client."""
        self.client = TestClient(app)
    
    @patch('app.routes.database.get_database_service')
    def test_test_database_connection_success(self, mock_get_db_service):
        """Test successful database connection endpoint."""
        # Arrange
        mock_db_service = Mock()
        mock_db_service.test_connection.return_value = True
        mock_get_db_service.return_value = mock_db_service
        
        # Act
        response = self.client.get("/database/test-connection")
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["database_connected"] is True
        assert "successful" in data["message"]
    
    @patch('app.routes.database.get_database_service')
    def test_test_database_connection_failure(self, mock_get_db_service):
        """Test failed database connection endpoint."""
        # Arrange
        mock_db_service = Mock()
        mock_db_service.test_connection.return_value = False
        mock_get_db_service.return_value = mock_db_service
        
        # Act
        response = self.client.get("/database/test-connection")
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["database_connected"] is False
        assert "failed" in data["message"]
    
    @patch('app.routes.database.get_database_service')
    def test_get_pipelines(self, mock_get_db_service):
        """Test get pipelines endpoint."""
        # Arrange
        mock_db_service = Mock()
        mock_pipelines = [
            (1, 'test_pipeline', 'Test description', {'type': 'etl'}, '# code', 'active', '2023-01-01T00:00:00', '2023-01-01T00:00:00')
        ]
        mock_db_service.fetch_all.return_value = mock_pipelines
        mock_get_db_service.return_value = mock_db_service
        
        # Act
        response = self.client.get("/database/pipelines")
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert "pipelines" in data
        assert data["count"] == 1
        assert data["pipelines"][0]["name"] == "test_pipeline"
    
    @patch('app.routes.database.get_database_service')
    def test_get_pipeline_by_id(self, mock_get_db_service):
        """Test get specific pipeline endpoint."""
        # Arrange
        mock_db_service = Mock()
        mock_pipeline = (1, 'test_pipeline', 'Test description', {'type': 'etl'}, '# code', 'active', '2023-01-01T00:00:00', '2023-01-01T00:00:00')
        mock_db_service.fetch_one.return_value = mock_pipeline
        mock_get_db_service.return_value = mock_db_service
        
        # Act
        response = self.client.get("/database/pipelines/1")
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert "pipeline" in data
        assert data["pipeline"]["name"] == "test_pipeline"
    
    @patch('app.routes.database.get_database_service')
    def test_get_pipeline_not_found(self, mock_get_db_service):
        """Test get non-existent pipeline."""
        # Arrange
        mock_db_service = Mock()
        mock_db_service.fetch_one.return_value = None
        mock_get_db_service.return_value = mock_db_service
        
        # Act
        response = self.client.get("/database/pipelines/999")
        
        # Assert
        assert response.status_code == 404
    
    @patch('app.routes.database.get_database_service')
    def test_get_chat_history(self, mock_get_db_service):
        """Test get chat history endpoint."""
        # Arrange
        mock_db_service = Mock()
        mock_chat_entries = [
            (1, 'session1', 'Hello', 'Hi there!', '2023-01-01T00:00:00')
        ]
        mock_db_service.fetch_all.return_value = mock_chat_entries
        mock_get_db_service.return_value = mock_db_service
        
        # Act
        response = self.client.get("/database/chat-history")
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert "chat_history" in data
        assert data["count"] == 1
        assert data["chat_history"][0]["user_message"] == "Hello"
