import os
import pandas as pd
import psycopg2
import logging
from .base_loader import BaseLoader

logger = logging.getLogger(__name__)

class CSVLoader(BaseLoader):
    """Loads CSV data files"""
    
    def load_data(self):
        """Load CSV files from the data directory"""
        logger.info("Loading CSV data files...")
        
        # Look for CSV files in the data directory (not just csv subdirectory)
        data_path = self.settings.data_directory
        if not os.path.exists(data_path):
            logger.warning(f"Data directory not found: {data_path}")
            return
        
        # Find all CSV files
        csv_files = [f for f in os.listdir(data_path) if f.endswith('.csv')]
        
        if not csv_files:
            logger.warning("No CSV files found")
            return
        
        for csv_file in csv_files:
            try:
                file_path = os.path.join(data_path, csv_file)
                logger.info(f"Processing CSV file: {csv_file}")
                
                # Read CSV file
                df = pd.read_csv(file_path)
                
                # Validate data
                self.validate_data(df)
                
                # Process the data based on filename
                if csv_file == 'bank_transactions.csv':
                    self._load_bank_transactions(df)
                else:
                    self._process_generic_csv_data(csv_file, df)
                
            except Exception as e:
                logger.error(f"Error processing CSV file {csv_file}: {e}")
    
    def _load_bank_transactions(self, df):
        """Load bank transactions CSV into the database"""
        logger.info(f"Loading {len(df)} bank transactions into database...")
        
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
                # Check if data already exists
                cursor.execute("SELECT COUNT(*) FROM bank_transactions")
                existing_count = cursor.fetchone()[0]
                
                if existing_count > 0:
                    logger.info(f"Bank transactions table already contains {existing_count} records. Skipping CSV load.")
                    return
                
                # Prepare the insert statement
                insert_sql = """
                INSERT INTO bank_transactions (
                    transaction_id, user_id, account_id, transaction_date, transaction_time,
                    amount, currency, merchant, category, transaction_type, status,
                    location, device, balance_after, notes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING
                """
                
                # Insert data in batches
                batch_size = 1000
                total_inserted = 0
                
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i:i + batch_size]
                    batch_data = []
                    
                    for _, row in batch.iterrows():
                        # Handle empty notes field
                        notes = row['notes'] if pd.notna(row['notes']) and row['notes'] != '' else None
                        
                        batch_data.append((
                            row['transaction_id'],
                            int(row['user_id']),
                            int(row['account_id']),
                            row['transaction_date'],
                            row['transaction_time'],
                            float(row['amount']),
                            row['currency'],
                            row['merchant'],
                            row['category'],
                            row['transaction_type'],
                            row['status'],
                            row['location'],
                            row['device'],
                            float(row['balance_after']),
                            notes
                        ))
                    
                    cursor.executemany(insert_sql, batch_data)
                    total_inserted += len(batch_data)
                    logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch_data)} records (Total: {total_inserted})")
                
                conn.commit()
                logger.info(f"Successfully loaded {total_inserted} bank transactions into database")
                
        except Exception as e:
            logger.error(f"Error loading bank transactions: {e}")
            if 'conn' in locals():
                conn.rollback()
            raise
        finally:
            if 'conn' in locals():
                conn.close()
    
    def _process_generic_csv_data(self, filename, dataframe):
        """Process other CSV files - implement your specific logic here"""
        logger.info(f"Processing {len(dataframe)} rows from {filename}")
        
        # This is where you'd implement logic for other CSV files
        # For now, just log that we processed it
        
        logger.info(f"Successfully processed {filename}")
