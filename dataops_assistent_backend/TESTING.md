# Unit Testing Guide for DataOps Assistant Backend

## Quick Start

1. **Install Testing Dependencies:**

   ```bash
   pip install pytest pytest-asyncio pytest-mock httpx pytest-cov
   ```

2. **Run All Tests:**

   ```bash
   make test
   # or
   python -m pytest tests/ -v
   ```

3. **Run Only Unit Tests:**

   ```bash
   make test-unit
   # or
   python -m pytest tests/ -v -m "unit"
   ```

4. **Run Tests with Coverage:**
   ```bash
   make test-coverage
   # or
   python -m pytest tests/ --cov=app --cov-report=term-missing
   ```

## Testing Structure

```
tests/
├── conftest.py          # Test fixtures and configuration
├── test_main.py         # Tests for main FastAPI app
├── test_integration.py  # Integration tests
├── routes/
│   └── test_chat.py     # Tests for chat routes
└── services/
    ├── test_chat_service.py
    └── test_pipeline_builder_service.py
```

## Key Testing Features

### 1. **Test Fixtures** (in conftest.py)

- `client`: FastAPI test client
- `async_client`: Async test client
- Mock services for external dependencies

### 2. **Mocking External Dependencies**

- MinIO storage service
- OpenAI LLM service
- Prompt guard service
- Pipeline builder service

### 3. **Test Categories**

- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test API endpoints end-to-end

### 4. **Coverage Reporting**

- HTML reports generated in `htmlcov/`
- Terminal coverage summary
- Configurable coverage thresholds

## Make Commands

- `make help`: Show available commands
- `make install`: Install dependencies
- `make test`: Run all tests
- `make test-unit`: Run unit tests only
- `make test-integration`: Run integration tests only
- `make test-coverage`: Run tests with coverage
- `make test-file FILE=tests/test_main.py`: Run specific test file
- `make test-pattern PATTERN="health"`: Run tests matching pattern
- `make clean`: Clean cache files

## Running Tests in CI/CD

Use the provided `run_tests.sh` script for automated testing:

```bash
./run_tests.sh
```

This script:

1. Installs dependencies
2. Runs linting (optional)
3. Runs unit tests
4. Runs integration tests
5. Generates coverage report

## Writing New Tests

### Unit Test Example:

```python
import pytest
from unittest.mock import Mock, patch

@pytest.mark.unit
class TestMyService:
    @patch('app.services.my_service.ExternalDependency')
    def test_my_function(self, mock_external):
        # Arrange
        mock_external.return_value.method.return_value = "test"

        # Act
        result = my_function()

        # Assert
        assert result == "expected"
```

### Integration Test Example:

```python
@pytest.mark.integration
def test_api_endpoint(client):
    response = client.post("/api/endpoint", json={"data": "test"})
    assert response.status_code == 200
```

## Best Practices

1. **Mock External Dependencies**: Always mock external services, databases, APIs
2. **Use Fixtures**: Reuse common test setup through fixtures
3. **Descriptive Test Names**: Test names should describe what they're testing
4. **Arrange-Act-Assert**: Structure tests clearly
5. **Test Edge Cases**: Include tests for error conditions and edge cases
6. **Keep Tests Fast**: Unit tests should run quickly
7. **Independent Tests**: Each test should be able to run independently
