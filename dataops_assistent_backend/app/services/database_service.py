"""
Database service for PostgreSQL operations.
"""
import os
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import logging

logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://dataops_user:dataops_password@localhost:5432/dataops_db")

# SQLAlchemy setup
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class DatabaseService:
    """Service for database operations."""
    
    def __init__(self):
        self.engine = engine
        self.SessionLocal = SessionLocal
    
    def get_db_session(self):
        """Get a database session."""
        db = self.SessionLocal()
        try:
            yield db
        finally:
            db.close()
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text("SELECT 1"))
                return result.fetchone()[0] == 1
        except SQLAlchemyError as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def execute_query(self, query: str, params: dict = None):
        """Execute a raw SQL query."""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query), params or {})
                connection.commit()
                return result
        except SQLAlchemyError as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def fetch_all(self, query: str, params: dict = None):
        """Fetch all results from a query."""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query), params or {})
                return result.fetchall()
        except SQLAlchemyError as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def fetch_one(self, query: str, params: dict = None):
        """Fetch one result from a query."""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query), params or {})
                return result.fetchone()
        except SQLAlchemyError as e:
            logger.error(f"Query execution failed: {e}")
            raise


# Global database service instance
database_service = DatabaseService()


def get_database_service() -> DatabaseService:
    """Get the database service instance."""
    return database_service
