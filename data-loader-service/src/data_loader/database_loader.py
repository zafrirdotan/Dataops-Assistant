import psycopg2
import logging
from .base_loader import BaseLoader

logger = logging.getLogger(__name__)

class DatabaseLoader(BaseLoader):
    """Loads database schema and initial data"""
    
    def load_data(self):
        """Initialize database schema and load initial data"""
        logger.info("Initializing database...")
        
        try:
            # Connect to database
            conn = psycopg2.connect(
                host=self.settings.database_host,
                port=self.settings.database_port,
                dbname=self.settings.database_name,
                user=self.settings.database_user,
                password=self.settings.database_password
            )
            
            with conn.cursor() as cursor:
                # Create tables
                self._create_tables(cursor)
                
                # Load initial data
                self._load_initial_data(cursor)
                
            conn.commit()
            logger.info("Database initialization completed successfully")
            
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            if 'conn' in locals():
                conn.rollback()
            raise
        finally:
            if 'conn' in locals():
                conn.close()
    
    def _create_tables(self, cursor):
        """Create database tables"""
        logger.info("Creating database tables...")
        
        # Example table creation - customize based on your needs
        create_customers_table = """
        CREATE TABLE IF NOT EXISTS customers (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        create_transactions_table = """
        CREATE TABLE IF NOT EXISTS bank_transactions (
            transaction_id VARCHAR(20) PRIMARY KEY,
            user_id INTEGER,
            account_id INTEGER,
            transaction_date DATE,
            transaction_time TIME,
            amount DECIMAL(10,2),
            currency VARCHAR(3),
            merchant VARCHAR(100),
            category VARCHAR(50),
            transaction_type VARCHAR(20),
            status VARCHAR(20),
            location VARCHAR(100),
            device VARCHAR(20),
            balance_after DECIMAL(12,2),
            notes TEXT
        );
        """
        
        cursor.execute(create_customers_table)
        cursor.execute(create_transactions_table)
        
        logger.info("Database tables created successfully")
    
    def _load_initial_data(self, cursor):
        """Load initial data into tables"""
        logger.info("Loading initial data...")
        
        # Check if data already exists
        cursor.execute("SELECT COUNT(*) FROM customers")
        customer_count = cursor.fetchone()[0]
        
        if customer_count > 0:
            logger.info(f"Database already contains {customer_count} customers. Skipping initial data load.")
            return
        
        # Example initial data - customize based on your needs
        initial_customers = [
            ('John Doe', 'john@example.com'),
            ('Jane Smith', 'jane@example.com'),
            ('Bob Johnson', 'bob@example.com')
        ]
        
        for name, email in initial_customers:
            cursor.execute(
                "INSERT INTO customers (name, email) VALUES (%s, %s) ON CONFLICT (email) DO NOTHING",
                (name, email)
            )
        
        logger.info(f"Initial data loaded successfully - inserted {len(initial_customers)} customers")
