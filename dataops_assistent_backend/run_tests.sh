#!/bin/bash

# Test script for CI/CD pipeline
set -e

echo "Starting backend tests..."

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Run linting (optional, requires additional packages)
echo "Running linting..."
pip install flake8 black isort
flake8 app/ --max-line-length=88 --ignore=E203,W503 || echo "Linting completed with warnings"

# Run unit tests
echo "Running unit tests..."
python -m pytest tests/ -v -m "unit" --tb=short

# Run integration tests
echo "Running integration tests..."
python -m pytest tests/ -v -m "integration" --tb=short

# Run all tests with coverage
echo "Running full test suite with coverage..."
python -m pytest tests/ --cov=app --cov-report=term-missing --cov-fail-under=70

echo "All tests completed successfully!"
