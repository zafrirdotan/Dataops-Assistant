import pytest
from unittest.mock import Mock, patch
import sys
import os

# Add the app directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))


@pytest.mark.unit
class TestChatService:
    """Test cases for the ChatService."""

    @patch('app.services.chat_service.PipelineBuilderService')
    @patch('app.services.chat_service.PromptGuardService')
    @patch('app.services.chat_service.LLMService')
    def test_chat_service_initialization(self, mock_llm, mock_guard, mock_pipeline):
        """Test ChatService initialization."""
        from app.services.chat_service import ChatService
        
        service = ChatService()
        
        assert service.llm_service is not None
        assert service.prompt_guard_service is not None
        assert service.pipeline_builder_service is not None

    @patch('app.services.chat_service.PipelineBuilderService')
    @patch('app.services.chat_service.PromptGuardService')
    @patch('app.services.chat_service.LLMService')
    def test_process_message_allow(self, mock_llm, mock_guard, mock_pipeline):
        """Test process_message with allowed input."""
        from app.services.chat_service import ChatService
        
        # Arrange
        mock_guard_instance = Mock()
        mock_pipeline_instance = Mock()
        mock_guard.return_value = mock_guard_instance
        mock_pipeline.return_value = mock_pipeline_instance
        
        mock_guard_instance.analyze.return_value = {
            "decision": "allow",
            "cleaned": "create a data pipeline",
            "findings": []
        }
        mock_pipeline_instance.build_pipeline.return_value = {
            "pipeline_name": "test_pipeline",
            "code": "# Test code"
        }
        
        service = ChatService()
        
        # Act
        result = service.process_message("create a data pipeline")
        
        # Assert
        assert result["decision"] == "allow"
        assert "response" in result
        mock_guard_instance.analyze.assert_called_once_with("create a data pipeline")
        mock_pipeline_instance.build_pipeline.assert_called_once_with("create a data pipeline")

    @patch('app.services.chat_service.PipelineBuilderService')
    @patch('app.services.chat_service.PromptGuardService')
    @patch('app.services.chat_service.LLMService')
    def test_process_message_block(self, mock_llm, mock_guard, mock_pipeline):
        """Test process_message with blocked input."""
        from app.services.chat_service import ChatService
        
        # Arrange
        mock_guard_instance = Mock()
        mock_guard.return_value = mock_guard_instance
        
        mock_guard_instance.analyze.return_value = {
            "decision": "block",
            "findings": ["malicious_code"]
        }
        
        service = ChatService()
        
        # Act
        result = service.process_message("rm -rf /")
        
        # Assert
        assert result["decision"] == "block"
        assert result["error"] == "Input blocked due to security concerns."
        assert result["findings"] == ["malicious_code"]

    @patch('app.services.chat_service.PipelineBuilderService')
    @patch('app.services.chat_service.PromptGuardService')
    @patch('app.services.chat_service.LLMService')
    def test_process_message_review_python_import_only(self, mock_llm, mock_guard, mock_pipeline):
        """Test process_message with review decision for python_import only."""
        from app.services.chat_service import ChatService
        
        # Arrange
        mock_guard_instance = Mock()
        mock_pipeline_instance = Mock()
        mock_guard.return_value = mock_guard_instance
        mock_pipeline.return_value = mock_pipeline_instance
        
        mock_guard_instance.analyze.return_value = {
            "decision": "review",
            "cleaned": "import pandas",
            "findings": [{"rule": "python_import"}]
        }
        mock_pipeline_instance.build_pipeline.return_value = {
            "pipeline_name": "test_pipeline"
        }
        
        service = ChatService()
        
        # Act
        result = service.process_message("import pandas")
        
        # Assert
        assert result["decision"] == "allow"
        mock_pipeline_instance.build_pipeline.assert_called_once()

    @patch('app.services.chat_service.PipelineBuilderService')
    @patch('app.services.chat_service.PromptGuardService')
    @patch('app.services.chat_service.LLMService')
    def test_process_message_review_multiple_findings(self, mock_llm, mock_guard, mock_pipeline):
        """Test process_message with review decision for multiple findings."""
        from app.services.chat_service import ChatService
        
        # Arrange
        mock_guard_instance = Mock()
        mock_guard.return_value = mock_guard_instance
        
        mock_guard_instance.analyze.return_value = {
            "decision": "review",
            "findings": [{"rule": "python_import"}, {"rule": "suspicious_pattern"}]
        }
        
        service = ChatService()
        
        # Act
        result = service.process_message("suspicious input")
        
        # Assert
        assert result["decision"] == "review"
        assert result["error"] == "Input requires review."
