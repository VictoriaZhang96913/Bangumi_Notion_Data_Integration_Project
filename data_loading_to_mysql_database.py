import pandas as pd
import numpy as np
import logging
import sys
import contextlib
import mysql.connector
from mysql.connector import connect
from mysql.connector import Error as MySQLError
from sqlalchemy import create_engine
import json

# Import the bangumi data ingestion module
import bangumi_data_ingestion as bangumi_data_ingestion

# Configure logging with UTF-8 encoding to handle Japanese/Chinese characters
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Log to console with UTF-8 encoding
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
# Force UTF-8 encoding for Windows console
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
logger.addHandler(handler)

# Also log to a file with UTF-8 encoding
file_handler = logging.FileHandler("data-loading-errors.log", encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler) 

# Database configuration
DB_CONFIGS = {
    "user": "",
    "password": "",
    "host": "",
    "database": "bangumi_analytics",
    "autocommit": False,
    "port": 3306,
}

# Connection manager
@contextlib.contextmanager
def connection_manager(configs):
    """Context manager for MySQL database connections"""
    connection = None
    try:
        connection = connect(
            user=configs["user"],
            password=configs["password"],
            host=configs["host"],
            database=configs["database"],
            autocommit=configs["autocommit"],
            port=configs["port"],
            charset='utf8mb4'  # Support full Unicode including emoji
        )
        yield connection
    except MySQLError as e:
        logger.error(f"MySQL Error: {e}")
        if connection:
            connection.rollback()
        raise
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        if connection:
            connection.close()


def prepare_dataframe_for_mysql(df):
    """
    Prepare DataFrame for MySQL insertion by converting incompatible types
    
    Args:
        df: pandas DataFrame
        
    Returns:
        DataFrame with MySQL-compatible types
    """
    df_copy = df.copy()
    
    # Convert list columns to JSON strings
    for col in df_copy.columns:
        if df_copy[col].dtype == 'object':
            # Check if column contains lists
            sample = df_copy[col].dropna().head(1)
            if len(sample) > 0 and isinstance(sample.iloc[0], list):
                logger.info(f"Converting list column '{col}' to JSON string")
                df_copy[col] = df_copy[col].apply(
                    lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else x
                )
    
    return df_copy


def load_bangumi_data_to_mysql(table_name='fact_view_logs', if_exists='replace'):
    """
    Load Bangumi data into MySQL database
    
    Args:
        table_name: Name of the target table in MySQL
        if_exists: How to behave if table exists ('fail', 'replace', 'append')
    
    Returns:
        tuple: (df_raw, df_analytics, all_stats) if successful, (None, None, None) otherwise
    """
    try:
        logger.info("Starting Bangumi data collection...")
        
        # Call the main function from bangumi_data_ingestion to get dataframes
        df_raw, df_analytics, all_stats = bangumi_data_ingestion.main()
        
        if df_raw is None or df_analytics is None:
            logger.error("Failed to collect Bangumi data")
            return None, None, None
        
        logger.info(f"Successfully collected {len(df_raw)} raw records and {len(df_analytics)} analytics records")
        
        # Create SQLAlchemy engine with UTF-8 support
        configs = DB_CONFIGS
        engine = create_engine(
            f'mysql+pymysql://{configs["user"]}:{configs["password"]}@{configs["host"]}:{configs["port"]}/{configs["database"]}?charset=utf8mb4'
        )
        
        # Drop unnecessary columns from analytics
        logger.info("Preparing analytics data...")
        columns_to_drop = [
            "director", "studio", "country", "publisher", "author",
            "tag_1_name", "tag_1_count", "tag_2_name", "tag_2_count",
            "tag_3_name", "tag_3_count", "tag_4_name", "tag_4_count",
            "tag_5_name", "tag_5_count"
        ]
        
        # Only drop columns that exist
        existing_columns = [col for col in columns_to_drop if col in df_analytics.columns]
        if existing_columns:
            df_analytics = df_analytics.drop(existing_columns, axis=1)
            logger.info(f"Dropped columns: {existing_columns}")
        
        # Load df_analytics to MySQL
        logger.info(f"Loading analytics data to MySQL table '{table_name}'...")
        
        # Prepare analytics data for MySQL
        df_analytics_prepared = prepare_dataframe_for_mysql(df_analytics)
        
        df_analytics_prepared.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            chunksize=1000
        )
        
        logger.info(f"Successfully loaded {len(df_analytics_prepared)} records to '{table_name}'")
        
        # Load raw data to a separate table
        raw_table_name = f"{table_name}_raw"
        logger.info(f"Loading raw data to MySQL table '{raw_table_name}'...")
        
        # Prepare raw data for MySQL (convert list columns)
        df_raw_prepared = prepare_dataframe_for_mysql(df_raw)
        
        df_raw_prepared.to_sql(
            name=raw_table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            chunksize=1000
        )
        
        logger.info(f"Successfully loaded {len(df_raw_prepared)} raw records to '{raw_table_name}'")
        
        return df_raw, df_analytics, all_stats
        
    except Exception as e:
        logger.error(f"Error in load_bangumi_data_to_mysql: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None


def implement_incremental_load(source_table='fact_view_logs', target_table='fact_view_logs_incremental'):
    """
    Implement incremental loading pattern for Bangumi data
    Compares source and target tables and syncs changes
    
    Args:
        source_table: Name of the source table (full data)
        target_table: Name of the target incremental table
    """
    try:
        configs = DB_CONFIGS
        engine = create_engine(
            f'mysql+pymysql://{configs["user"]}:{configs["password"]}@{configs["host"]}:{configs["port"]}/{configs["database"]}?charset=utf8mb4'
        )
        
        # Read source data (full load)
        logger.info(f"Reading source data from '{source_table}'...")
        df_source = pd.read_sql(f"SELECT * FROM {source_table}", con=engine)
        
        # Try to read target data (incremental)
        try:
            logger.info(f"Reading target data from '{target_table}'...")
            df_target = pd.read_sql(f"SELECT * FROM {target_table}", con=engine)
        except Exception as e:
            logger.info(f"Target table '{target_table}' doesn't exist or is empty. Performing initial load.")
            df_target = pd.DataFrame()
        
        # Handle initial load
        if df_target.empty:
            logger.info("Initial load - target is empty, loading all data from source")
            df_source.to_sql(name=target_table, con=engine, if_exists='replace', index=False, chunksize=1000)
            logger.info(f"Initial load complete. Loaded {len(df_source)} records")
            return
        
        # Incremental sync logic using subject_id as primary key
        id_column = 'subject_id'
        
        # Separate records with valid IDs from those with NA IDs
        df_source_with_id = df_source[df_source[id_column].notna()].copy()
        df_source_na_id = df_source[df_source[id_column].isna()].copy()
        
        df_target_with_id = df_target[df_target[id_column].notna()].copy()
        df_target_na_id = df_target[df_target[id_column].isna()].copy()
        
        # Log NA occurrences for data quality monitoring
        if not df_source_na_id.empty:
            logger.warning(f"Found {len(df_source_na_id)} records with NA IDs in source")
        if not df_target_na_id.empty:
            logger.warning(f"Found {len(df_target_na_id)} records with NA IDs in target")
        
        # Calculate inserts and deletes using sets for efficiency
        source_id_set = set(df_source_with_id[id_column])
        target_id_set = set(df_target_with_id[id_column])
        
        # Insert: IDs in source but not in target
        insert_ids = source_id_set - target_id_set
        
        # Delete: IDs in target but not in source
        delete_ids = target_id_set - source_id_set
        
        # Update: IDs in both (check if data changed)
        common_ids = source_id_set & target_id_set
        
        logger.info(f"Inserts: {len(insert_ids)} records")
        logger.info(f"Deletes: {len(delete_ids)} records")
        logger.info(f"Potential updates: {len(common_ids)} records")
        
        # Start with target data (excluding records to be deleted)
        df_combined = df_target_with_id[~df_target_with_id[id_column].isin(delete_ids)].copy()
        
        # Remove old versions of records that will be updated
        df_combined = df_combined[~df_combined[id_column].isin(common_ids)].copy()
        
        # Add new records from source (inserts)
        df_new = df_source_with_id[df_source_with_id[id_column].isin(insert_ids)].copy()
        
        # Add updated records from source
        df_updated = df_source_with_id[df_source_with_id[id_column].isin(common_ids)].copy()
        
        # Combine all data
        df_combined = pd.concat([df_combined, df_new, df_updated, df_source_na_id], ignore_index=True)
        
        # Write to database
        df_combined.to_sql(name=target_table, con=engine, if_exists='replace', index=False, chunksize=1000)
        
        logger.info(f"Incremental sync complete. Final record count: {len(df_combined)}")
        logger.info(f"Net change: {len(insert_ids) - len(delete_ids)} records")
        
    except Exception as e:
        logger.error(f"Error in implement_incremental_load: {e}")
        import traceback
        traceback.print_exc()
        raise


def main():
    """
    Main execution function
    Loads Bangumi data and implements incremental loading
    """
    logger.info("="*60)
    logger.info("Starting Bangumi Data Loading Process")
    logger.info("="*60)
    
    # Step 1: Collect and load Bangumi data to MySQL
    df_raw, df_analytics, all_stats = load_bangumi_data_to_mysql(
        table_name='fact_view_logs',
        if_exists='replace'
    )
    
    if df_raw is None:
        logger.error("Data collection failed. Exiting.")
        return
    
    # Step 2: Implement incremental loading pattern
    logger.info("\n" + "="*60)
    logger.info("Implementing Incremental Load")
    logger.info("="*60)
    
    implement_incremental_load(
        source_table='fact_view_logs',
        target_table='fact_view_logs_incremental'
    )
    
    logger.info("\n" + "="*60)
    logger.info("Data Loading Process Complete")
    logger.info("="*60)
    
    return df_raw, df_analytics, all_stats


if __name__ == "__main__":
    main()