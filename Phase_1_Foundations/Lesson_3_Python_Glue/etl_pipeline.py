# etl_pipeline.py
# Purpose: This script demonstrates a modular, architect-approved Python ETL pattern.

import pandas as pd
import logging
import sqlite3
from datetime import datetime

# 1. SETUP LOGGING (Architects need logs to debug)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DataPipeline")

def extract_data(source_path):
    """Simplified extraction logic."""
    try:
        logger.info(f"Extracting data from {source_path}...")
        # Imagine this is a large CSV or API call
        df = pd.read_csv(source_path)
        return df
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise

def transform_data(df):
    """Simplified transformation logic."""
    try:
        logger.info("Transforming data...")
        # Cleaning: Remove nulls, formatting dates
        df = df.dropna(subset=['user_id'])
        df['processed_timestamp'] = datetime.now()
        # Business logic: Filter high-value transactions
        df = df[df['amount'] > 50] 
        return df
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise

def load_data(df, db_name="warehouse.db"):
    """Loading into a target (SQLite for demonstration)."""
    try:
        logger.info(f"Loading data into {db_name}...")
        conn = sqlite3.connect(db_name)
        df.to_sql('cleansed_transactions', conn, if_exists='append', index=False)
        conn.close()
        logger.info("Load complete.")
    except Exception as e:
        logger.error(f"Load failed: {e}")
        raise

if __name__ == "__main__":
    # In a real scenario, these paths would be configuration-driven
    try:
        raw_df = extract_data("sample_data.csv") # Assuming sample_data.csv exists
        transformed_df = transform_data(raw_df)
        load_data(transformed_df)
    except Exception:
        logger.critical("Pipeline execution aborted due to critical error.")
