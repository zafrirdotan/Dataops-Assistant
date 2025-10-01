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
            logger.info("Testing database connection...")
            logger.info(f"Using DATABASE_URL: {DATABASE_URL}")
            
            # Test the connection with explicit transaction handling
            connection = self.engine.connect()
            try:
                result = connection.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                success = row is not None and row[0] == 1
                logger.info(f"Connection test result: {success}")
                return success
            finally:
                connection.close()
                
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            logger.error(f"Exception type: {type(e).__name__}")
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

    def get_connection_info(self) -> dict:
        """Get detailed connection information for debugging."""
        try:
            with self.engine.connect() as connection:
                # Get database version and connection info
                result = connection.execute(text("SELECT version()"))
                db_version = result.fetchone()[0]
                
                result = connection.execute(text("SELECT current_database(), current_user, inet_server_addr(), inet_server_port()"))
                conn_info = result.fetchone()
                
                return {
                    "connected": True,
                    "database_version": db_version,
                    "current_database": conn_info[0],
                    "current_user": conn_info[1],
                    "server_address": conn_info[2],
                    "server_port": conn_info[3],
                    "connection_url": DATABASE_URL.replace(DATABASE_URL.split('@')[0].split('//')[1], '***:***')  # Hide credentials
                }
        except Exception as e:
            return {
                "connected": False,
                "error": str(e),
                "error_type": type(e).__name__,
                "connection_url": DATABASE_URL.replace(DATABASE_URL.split('@')[0].split('//')[1], '***:***')
            }


# Global database service instance
database_service = DatabaseService()


def get_database_service() -> DatabaseService:
    """Get the database service instance."""
    return database_service
