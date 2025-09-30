import pytest
from unittest.mock import Mock, patch
import sys
import os

# Add the app directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))


@pytest.mark.unit
class TestChatRoute:
    """Test cases for the chat route."""

    # @patch('app.routes.chat.ChatService')
    # def test_chat_endpoint_success(self, mock_chat_service, client):
    #     """Test successful chat endpoint response."""
    #     # Arrange
    #     mock_service_instance = Mock()
    #     mock_chat_service.return_value = mock_service_instance
    #     mock_service_instance.process_message.return_value = {
    #         "decision": "allow",
    #         "response": {"message": "Test response", "pipeline": "test_pipeline"}
    #     }

    #     # Act
    #     response = client.post("/chat", json={"message": "Create a pipeline"})

    #     # Assert
    #     assert response.status_code == 200
    #     data = response.json()
    #     assert "response" in data
    #     print("data:", data)
    #     assert data["response"]["success"] == True

    # @patch('app.routes.chat.ChatService')
    # def test_chat_endpoint_blocked(self, mock_chat_service, client):
    #     """Test chat endpoint when message is blocked."""
    #     # Arrange
    #     mock_service_instance = Mock()
    #     mock_chat_service.return_value = mock_service_instance
    #     mock_service_instance.process_message.return_value = {
    #         "decision": "block",
    #         "error": "Input blocked due to security concerns.",
    #         "findings": ["malicious_code"]
    #     }

    #     # Act
    #     response = client.post("/chat", json={"message": "malicious input"})

    #     # Assert
    #     assert response.status_code == 400
    #     data = response.json()
    #     assert "detail" in data
    #     assert data["detail"]["decision"] == "block"

    # @patch('app.routes.chat.ChatService')
    # def test_chat_endpoint_review(self, mock_chat_service, client):
    #     """Test chat endpoint when message requires review."""
    #     # Arrange
    #     mock_service_instance = Mock()
    #     mock_chat_service.return_value = mock_service_instance
    #     mock_service_instance.process_message.return_value = {
    #         "decision": "review",
    #         "error": "Input requires review.",
    #         "findings": ["suspicious_pattern"]
    #     }

    #     # Act
    #     response = client.post("/chat", json={"message": "suspicious input"})

    #     # Assert
    #     assert response.status_code == 200
    #     data = response.json()
    #     assert data["decision"] == "review"

    def test_chat_endpoint_invalid_input(self, client):
        """Test chat endpoint with invalid input."""
        # Act
        response = client.post("/chat", json={})

        # Assert
        assert response.status_code == 422  # Validation error

    def test_chat_endpoint_empty_message(self, client):
        """Test chat endpoint with empty message."""
        # Act
        response = client.post("/chat", json={"message": ""})

        # Assert
        assert response.status_code == 200  # Should still process empty messages
