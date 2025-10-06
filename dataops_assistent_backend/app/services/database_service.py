"""
Database service for PostgreSQL operations.
"""
import os
import asyncio
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
import logging

logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://dataops_user:dataops_password@localhost:5432/dataops_db")
ASYNC_DATABASE_URL = os.getenv("ASYNC_DATABASE_URL", DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://"))

# SQLAlchemy setup
engine = create_engine(DATABASE_URL)
async_engine = create_async_engine(ASYNC_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
AsyncSessionLocal = async_sessionmaker(async_engine, expire_on_commit=False)
Base = declarative_base()


class DatabaseService:
    """Service for database operations."""
    
    def __init__(self):
        self.engine = engine
        self.async_engine = async_engine
        self.SessionLocal = SessionLocal
        self.AsyncSessionLocal = AsyncSessionLocal
    
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

    # Async methods
    async def get_async_db_session(self):
        """Get an async database session."""
        async with self.AsyncSessionLocal() as session:
            try:
                yield session
            finally:
                await session.close()
    
    async def test_connection(self) -> bool:
        """Test async database connection."""
        try:
            logger.info("Testing async database connection...")
            logger.info(f"Using ASYNC_DATABASE_URL: {ASYNC_DATABASE_URL}")
            
            # Test the connection with explicit transaction handling
            async with self.async_engine.connect() as connection:
                result = await connection.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                success = row is not None and row[0] == 1
                logger.info(f"Async connection test result: {success}")
                return success
                
        except Exception as e:
            logger.error(f"Async database connection test failed: {e}")
            logger.error(f"Exception type: {type(e).__name__}")
            # Fallback to sync version
            return self.test_connection_sync()
    
    def test_connection_sync(self) -> bool:
        """Test sync database connection (fallback)."""
        try:
            logger.info("Testing sync database connection...")
            logger.info(f"Using DATABASE_URL: {DATABASE_URL}")
            
            # Test the connection with explicit transaction handling
            connection = self.engine.connect()
            try:
                result = connection.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                success = row is not None and row[0] == 1
                logger.info(f"Sync connection test result: {success}")
                return success
            finally:
                connection.close()
                
        except Exception as e:
            logger.error(f"Sync database connection test failed: {e}")
            logger.error(f"Exception type: {type(e).__name__}")
            return False
    
    async def execute_query_async(self, query: str, params: dict = None):
        """Execute a raw SQL query asynchronously."""
        try:
            async with self.async_engine.connect() as connection:
                result = await connection.execute(text(query), params or {})
                await connection.commit()
                return result
        except SQLAlchemyError as e:
            logger.error(f"Async query execution failed: {e}")
            raise
    
    async def fetch_all(self, query: str, params: dict = None):
        """Fetch all results from a query asynchronously."""
        try:
            async with self.async_engine.connect() as connection:
                result = await connection.execute(text(query), params or {})
                return result.fetchall()
        except SQLAlchemyError as e:
            logger.error(f"Async query execution failed: {e}")
            # Fallback to sync version
            return self.fetch_all_sync(query, params)
    
    def fetch_all_sync(self, query: str, params: dict = None):
        """Fetch all results from a query synchronously (fallback)."""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query), params or {})
                return result.fetchall()
        except SQLAlchemyError as e:
            logger.error(f"Sync query execution failed: {e}")
            raise
    
    async def fetch_one_async(self, query: str, params: dict = None):
        """Fetch one result from a query asynchronously."""
        try:
            async with self.async_engine.connect() as connection:
                result = await connection.execute(text(query), params or {})
                return result.fetchone()
        except SQLAlchemyError as e:
            logger.error(f"Async query execution failed: {e}")
            raise

    async def get_connection_info_async(self) -> dict:
        """Get detailed connection information for debugging asynchronously."""
        try:
            async with self.async_engine.connect() as connection:
                # Get database version and connection info
                result = await connection.execute(text("SELECT version()"))
                db_version = result.fetchone()[0]
                
                result = await connection.execute(text("SELECT current_database(), current_user, inet_server_addr(), inet_server_port()"))
                conn_info = result.fetchone()
                
                return {
                    "connected": True,
                    "database_version": db_version,
                    "current_database": conn_info[0],
                    "current_user": conn_info[1],
                    "server_address": conn_info[2],
                    "server_port": conn_info[3],
                    "connection_url": ASYNC_DATABASE_URL.replace(ASYNC_DATABASE_URL.split('@')[0].split('//')[1], '***:***')  # Hide credentials
                }
        except Exception as e:
            return {
                "connected": False,
                "error": str(e),
                "error_type": type(e).__name__,
                "connection_url": ASYNC_DATABASE_URL.replace(ASYNC_DATABASE_URL.split('@')[0].split('//')[1], '***:***')
            }


# Global database service instance
database_service = DatabaseService()


def get_database_service() -> DatabaseService:
    """Get the database service instance."""
    return database_service
