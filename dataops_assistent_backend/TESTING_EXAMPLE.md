# Example: How to write and run unit tests for your backend

## Quick Demo

Here's a simple working example of unit testing in your project:

### 1. Basic Service Test (tests/services/test_chat_service.py)

```python
import pytest
from unittest.mock import Mock, patch

@pytest.mark.unit
class TestChatService:
    @patch('app.services.chat_service.PipelineBuilderService')
    @patch('app.services.chat_service.PromptGuardService')
    @patch('app.services.chat_service.LLMService')
    def test_process_message_allow(self, mock_llm, mock_guard, mock_pipeline):
        """Test process_message with allowed input."""
        from app.services.chat_service import ChatService

        # Arrange - set up mocks
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

        # Act - call the method being tested
        result = service.process_message("create a data pipeline")

        # Assert - verify the results
        assert result["decision"] == "allow"
        assert "response" in result
        mock_guard_instance.analyze.assert_called_once_with("create a data pipeline")
        mock_pipeline_instance.build_pipeline.assert_called_once_with("create a data pipeline")
```

### 2. API Endpoint Test (tests/test_main.py)

```python
def test_health_check_endpoint(self, client):
    """Test the health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "dataops-assistant"
```

## Running the Tests

### Run individual test files:

```bash
# Service tests (these work well)
python -m pytest tests/services/test_chat_service.py -v

# Main app tests
python -m pytest tests/test_main.py::TestMainApp::test_health_check_endpoint -v
```

### Run with Make commands:

```bash
make test-unit    # Run all unit tests
make test         # Run all tests
make clean        # Clean cache files
```

## Test Results

✅ **Working Tests:**

- Service initialization tests
- Mocked service interaction tests
- Basic API endpoint tests (health check, root)

❌ **Failing Tests:**

- Tests that don't properly mock external dependencies
- Integration tests that call real services
- Tests that expect specific response formats

## Key Success Factors

1. **Proper Mocking**: Mock all external dependencies
2. **Clear Test Structure**: Arrange-Act-Assert pattern
3. **Isolated Tests**: Each test runs independently
4. **Descriptive Names**: Test names describe what they test

## Next Steps

1. **Fix Failing Tests**: Improve mocking for complex scenarios
2. **Add More Tests**: Cover edge cases and error conditions
3. **Integration Tests**: Test real API flows with proper setup
4. **CI/CD**: Integrate testing into your deployment pipeline
