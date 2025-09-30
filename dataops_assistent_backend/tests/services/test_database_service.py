"""
Test script for PostgreSQL integration.
"""
import pytest
from unittest.mock import patch, Mock
from app.services.database_service import DatabaseService, get_database_service


class TestDatabaseService:
    """Test cases for the database service."""
    
    @patch('app.services.database_service.engine')
    def test_test_connection_success(self, mock_engine):
        """Test successful database connection."""
        # Arrange
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.fetchone.return_value = [1]
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        
        db_service = DatabaseService()
        
        # Act
        result = db_service.test_connection()
        
        # Assert
        assert result is True
        mock_connection.execute.assert_called_once()
    
    @patch('app.services.database_service.engine')
    def test_test_connection_failure(self, mock_engine):
        """Test failed database connection."""
        # Arrange
        mock_engine.connect.side_effect = Exception("Connection failed")
        
        db_service = DatabaseService()
        
        # Act
        result = db_service.test_connection()
        
        # Assert
        assert result is False
    
    @patch('app.services.database_service.engine')
    def test_fetch_all(self, mock_engine):
        """Test fetch_all method."""
        # Arrange
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.fetchall.return_value = [('test1',), ('test2',)]
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        
        db_service = DatabaseService()
        
        # Act
        result = db_service.fetch_all("SELECT * FROM test_table")
        
        # Assert
        assert result == [('test1',), ('test2',)]
        mock_connection.execute.assert_called_once()
    
    @patch('app.services.database_service.engine')
    def test_fetch_one(self, mock_engine):
        """Test fetch_one method."""
        # Arrange
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.fetchone.return_value = ('test1',)
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        
        db_service = DatabaseService()
        
        # Act
        result = db_service.fetch_one("SELECT * FROM test_table WHERE id = 1")
        
        # Assert
        assert result == ('test1',)
        mock_connection.execute.assert_called_once()
    
    def test_get_database_service(self):
        """Test get_database_service function."""
        # Act
        service = get_database_service()
        
        # Assert
        assert isinstance(service, DatabaseService)
