import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add the app directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))


@pytest.mark.unit
class TestPipelineBuilderService:
    """Test cases for the PipelineBuilderService."""

    @patch('app.services.pipeline_builder_service.TestPipelineService')
    @patch('app.services.pipeline_builder_service.PipelineCodeGeneratorDSPy')
    @patch('app.services.pipeline_builder_service.LocalFileService')
    @patch('app.services.pipeline_builder_service.PipelineSpecGenerator')
    @patch('app.services.pipeline_builder_service.LLMService')
    @patch('app.services.pipeline_builder_service.PromptGuardService')
    def test_pipeline_builder_initialization(self, mock_guard, mock_llm, mock_spec, 
                                           mock_file, mock_code, mock_test):
        """Test PipelineBuilderService initialization."""
        from app.services.pipeline_builder_service import PipelineBuilderService
        
        service = PipelineBuilderService()
        
        assert service.guard is not None
        assert service.llm is not None
        assert service.spec_gen is not None
        assert service.local_file_service is not None
        assert service.code_gen is not None
        assert service.test_service is not None

    @patch('app.services.pipeline_builder_service.TestPipelineService')
    @patch('app.services.pipeline_builder_service.PipelineCodeGeneratorDSPy')
    @patch('app.services.pipeline_builder_service.LocalFileService')
    @patch('app.services.pipeline_builder_service.PipelineSpecGenerator')
    @patch('app.services.pipeline_builder_service.LLMService')
    @patch('app.services.pipeline_builder_service.PromptGuardService')
    def test_build_pipeline_success(self, mock_guard, mock_llm, mock_spec, 
                                   mock_file, mock_code, mock_test):
        """Test successful pipeline building."""
        from app.services.pipeline_builder_service import PipelineBuilderService
        
        # Arrange
        mock_spec_instance = Mock()
        mock_code_instance = Mock()
        mock_file_instance = Mock()
        mock_test_instance = Mock()
        
        mock_spec.return_value = mock_spec_instance
        mock_code.return_value = mock_code_instance
        mock_file.return_value = mock_file_instance
        mock_test.return_value = mock_test_instance
        
        test_spec = {
            "pipeline_name": "test_pipeline",
            "source_type": "localFileCSV",
            "source_path": "test.csv"
        }
        
        mock_spec_instance.generate_spec.return_value = test_spec
        mock_code_instance.generate_code.return_value = ("# test code", "pandas", "# test")
        mock_test_instance.create_and_run_unittest.return_value = {"success": True}
        
        service = PipelineBuilderService()
        # Mock the methods
        service.validate_spec_schema = Mock(return_value=True)
        service.connect_to_source = Mock(return_value={"success": True, "data_preview": []})
        
        # Act
        result = service.build_pipeline("create a pipeline")
        
        # Assert
        assert result["success"] is True
        assert "spec" in result
        assert "code" in result
        mock_spec_instance.generate_spec.assert_called_once_with("create a pipeline")

    @patch('app.services.pipeline_builder_service.TestPipelineService')
    @patch('app.services.pipeline_builder_service.PipelineCodeGeneratorDSPy')
    @patch('app.services.pipeline_builder_service.LocalFileService')
    @patch('app.services.pipeline_builder_service.PipelineSpecGenerator')
    @patch('app.services.pipeline_builder_service.LLMService')
    @patch('app.services.pipeline_builder_service.PromptGuardService')
    def test_validate_source_path_csv(self, mock_guard, mock_llm, mock_spec,
                                     mock_file, mock_code, mock_test):
        """Test source path validation for CSV files."""
        from app.services.pipeline_builder_service import PipelineBuilderService
        
        service = PipelineBuilderService()
        
        # Test valid CSV path
        spec_csv = {"source_type": "localFileCSV", "source_path": "test.csv"}
        assert service.validate_source_path(spec_csv) is True
        
        # Test invalid CSV path
        spec_invalid = {"source_type": "localFileCSV", "source_path": "test.txt"}
        assert service.validate_source_path(spec_invalid) is False

    @patch('app.services.pipeline_builder_service.TestPipelineService')
    @patch('app.services.pipeline_builder_service.PipelineCodeGeneratorDSPy')
    @patch('app.services.pipeline_builder_service.LocalFileService')
    @patch('app.services.pipeline_builder_service.PipelineSpecGenerator')
    @patch('app.services.pipeline_builder_service.LLMService')
    @patch('app.services.pipeline_builder_service.PromptGuardService')
    def test_connect_to_source_local_csv_success(self, mock_guard, mock_llm, mock_spec,
                                                mock_file, mock_code, mock_test):
        """Test connecting to local CSV source successfully."""
        from app.services.pipeline_builder_service import PipelineBuilderService
        
        # Arrange
        mock_file_instance = Mock()
        mock_file.return_value = mock_file_instance
        
        mock_data = Mock()
        mock_data.head.return_value.to_dict.return_value = [{"col1": "val1"}]
        mock_file_instance.retrieve_recent_data_files.return_value = mock_data
        
        service = PipelineBuilderService()
        spec = {"source_type": "localFileCSV", "source_path": "test.csv"}
        
        # Act
        result = service.connect_to_source(spec)
        
        # Assert
        assert result["success"] is True
        assert "data_preview" in result
