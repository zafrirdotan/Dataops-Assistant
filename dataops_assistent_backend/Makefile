.PHONY: help install test test-unit test-integration test-coverage clean lint format

# Default target
help:
	@echo "Available commands:"
	@echo "  install          Install dependencies"
	@echo "  test             Run all tests"
	@echo "  test-unit        Run unit tests only"
	@echo "  test-integration Run integration tests only"
	@echo "  test-coverage    Run tests with coverage report"
	@echo "  test-watch       Run tests in watch mode"
	@echo "  lint             Run code linting"
	@echo "  format           Format code"
	@echo "  clean            Clean cache files"

# Install dependencies
install:
	pip3 install -r requirements.txt

# Run all tests
test:
	python3 -m pytest tests/ -v

# Run unit tests only
test-unit:
	python3 -m pytest tests/ -v -m "unit"

# Run integration tests only
test-integration:
	python3 -m pytest tests/ -v -m "integration"

# Run tests with coverage
test-coverage:
	python3 -m pytest tests/ --cov=app --cov-report=term-missing --cov-report=html

# Run tests in watch mode
test-watch:
	python3 -m pytest tests/ -v --tb=short -f

# Run specific test file
test-file:
	python3 -m pytest $(FILE) -v

# Run tests matching pattern
test-pattern:
	python3 -m pytest tests/ -v -k "$(PATTERN)"

# Lint code
lint:
	python3 -m flake8 app/ tests/
	python3 -m pylint app/

# Format code
format:
	python3 -m black app/ tests/
	python3 -m isort app/ tests/

# Clean cache files
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
