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
import os
import requests
import time
import data_loading_to_mysql_database as mysql_data_ingestion

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set environment variables
os.environ["notion_token"] = ""
os.environ["parent_id"] = ""
os.environ["database_id"] = ""  # FIXED: Add database_id

# Get environment variables (FIXED: was looking for wrong env var name)
notion_token = os.getenv("notion_token")  # Changed from "notion_api"
parent_id = os.getenv("parent_id")
database_id = os.getenv("database_id")  # FIXED: Get database_id from environment

# Global configuration dictionary
notion_config = {
    "notion_token": notion_token,
    "parent_id": parent_id,
    "database_id": database_id,  # FIXED: Add database_id to config
}

# FIXED: Headers should use the actual token value, not the key
headers = {
    "Authorization": f"Bearer {notion_token}",  # Changed from notion_config.get(notion_token)
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}


def create_page(parent_id, notion_token):
    """Create a parent page in Notion if it doesn't exist"""
    
    headers = {
        "Authorization": f"Bearer {notion_token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    if parent_id is None:
        logger.info("Creating new parent page...")
        response = requests.post(
            "https://api.notion.com/v1/pages",
            headers=headers,
            json={
                "parent": {"type": "workspace", "workspace": True},
                "properties": {
                    "title": {"title": [{"text": {"content": "Bangumi Data Import"}}]}
                }
            }
        )
        
        if response.status_code != 200:
            logger.error(f"Failed to create page: {response.text}")
            raise Exception(f"Failed to create page: {response.text}")
        
        parent_id = response.json()['id']
        notion_config["parent_id"] = parent_id  # FIXED: Use dict assignment, not append
        logger.info(f"✓ Created page: {parent_id}")
        return parent_id
    else:
        logger.info(f"✓ Using existing page: {parent_id}")
        return parent_id


def create_database(parent_id, notion_token, database_id):  # FIXED: Add database_id parameter
    """Create a database in Notion"""
    
    headers = {
        "Authorization": f"Bearer {notion_token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # FIXED: Check if database_id exists, if yes, skip creation
    if database_id is not None:
        logger.info(f"✓ Using existing database: {database_id}")
        return database_id
    
    logger.info("Creating Notion database...")
    
    # FIXED: Should use /databases endpoint, not /pages
    response = requests.post(
        "https://api.notion.com/v1/databases",  # Changed from /pages
        headers=headers,
        json={
            "parent": {"type": "page_id", "page_id": parent_id},
            "title": [{"type": "text", "text": {"content": "Bangumi Database"}}],
            "properties": {
                "subject_id": {"title": {}},  # Title property (required)
                "subject_type": {"number": {}},
                "collection_type": {"number": {}},
                "name_cn": {"rich_text": {}},  # FIXED: Changed from number to rich_text
                "score": {"number": {}},
                "rank": {"number": {}},
                "collection_total": {"number": {}},
                "created_at": {"rich_text": {}},  # FIXED: Changed from date to rich_text
                "updated_at": {"date": {}},
                "eps": {"number": {}},
                "air_date": {"rich_text": {}},  # FIXED: Changed from date to rich_text
                "all_tags": {"rich_text": {}}  # FIXED: Changed from title to rich_text
            }
        }
    )
    
    if response.status_code != 200:
        logger.error(f"Failed to create database: {response.text}")
        raise Exception(f"Failed to create database: {response.text}")
    
    database_data = response.json()
    database_id = database_data['id']
    logger.info(f"✓ Created database: {database_id}")
    
    notion_config["database_id"] = database_id  # FIXED: Use dict assignment
    return database_id


def get_existing_records(database_id, notion_token):
    """Retrieve all existing records from Notion database"""
    
    headers = {
        "Authorization": f"Bearer {notion_token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    logger.info("Fetching existing records from Notion database...")
    
    all_records = {}
    has_more = True
    start_cursor = None
    
    while has_more:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                payload = {"page_size": 100}
                if start_cursor:
                    payload["start_cursor"] = start_cursor
                
                response = requests.post(
                    f"https://api.notion.com/v1/databases/{database_id}/query",
                    headers=headers,
                    json=payload,
                    timeout=30
                )
                
                if response.status_code != 200:
                    logger.error(f"Failed to query database: {response.text}")
                    return {}
                
                data = response.json()
                
                # Extract records
                for page in data.get('results', []):
                    try:
                        # Get subject_id from title property
                        subject_id_prop = page['properties'].get('subject_id', {})
                        if subject_id_prop.get('title'):
                            subject_id = subject_id_prop['title'][0]['text']['content']
                            all_records[subject_id] = {
                                'page_id': page['id'],
                                'properties': page['properties']
                            }
                    except Exception as e:
                        logger.warning(f"Error parsing record: {e}")
                        continue
                
                has_more = data.get('has_more', False)
                start_cursor = data.get('next_cursor')
                
                logger.info(f"Fetched {len(all_records)} existing records so far...")
                
                break  # Success, exit retry loop
                
            except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
                logger.warning(f"SSL/Connection error on attempt {attempt + 1}/{max_retries}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    has_more = False
                    break
    
    logger.info(f"Total existing records fetched: {len(all_records)}")
    return all_records


def soft_delete_record(page_id, notion_token):
    """Soft delete a record by setting is_active to False"""
    
    headers = {
        "Authorization": f"Bearer {notion_token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.patch(
                f"https://api.notion.com/v1/pages/{page_id}",
                headers=headers,
                json={
                    "properties": {
                        "is_active": {"checkbox": False}
                    }
                },
                timeout=30
            )
            
            if response.status_code == 200:
                return True
            else:
                return False
                
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
            logger.warning(f"SSL/Connection error on attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            else:
                return False
        except Exception as e:
            return False
    
    return False


def format_property(property_name, value, property_type):
    """Format a value for Notion API based on property type"""
    
    if pd.isna(value) or value is None:
        return None
    
    if property_type == "title":
        return {
            "title": [
                {
                    "text": {
                        "content": str(value)[:2000]  # Notion limit
                    }
                }
            ]
        }
    elif property_type == "rich_text":
        return {
            "rich_text": [
                {
                    "text": {
                        "content": str(value)[:2000]
                    }
                }
            ]
        }
    elif property_type == "number":
        try:
            return {"number": float(value) if '.' in str(value) else int(value)}
        except:
            logger.warning(f"Could not convert {value} to number")
            return None
    elif property_type == "date":
        try:
            # Convert to ISO format if it's a datetime object
            if isinstance(value, pd.Timestamp):
                date_str = value.isoformat()
            else:
                date_str = str(value)
            return {"date": {"start": date_str}}
        except:
            logger.warning(f"Could not convert {value} to date")
            return None
    
    return None


def insert_data_into_database(parent_id, notion_token, database_id):
    """Insert data from MySQL into Notion database"""
    
    headers = {
        "Authorization": f"Bearer {notion_token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    logger.info("Loading data from MySQL...")
    
    # FIXED: Corrected function call
    df_raw, df_analytics, all_stats = mysql_data_ingestion.load_bangumi_data_to_mysql(
        table_name='fact_view_logs',
        if_exists='replace'
    )
    
    logger.info(f"Loaded {len(df_analytics)} rows from MySQL")
    logger.info("Starting insertion into Notion...")
    
    success_count = 0
    error_count = 0
    
    # FIXED: iterrows() not itterows()
    for index, row in df_analytics.iterrows():
        try:
            # Build properties dictionary
            properties = {}
            
            # Map each field with proper formatting
            # subject_id is the title field
            prop = format_property("subject_id", row.get("subject_id"), "title")
            if prop:
                properties["subject_id"] = prop
            else:
                properties["subject_id"] = {"title": [{"text": {"content": "Untitled"}}]}
            
            # Other number fields
            prop = format_property("subject_type", row.get("subject_type"), "number")
            if prop:
                properties["subject_type"] = prop
            
            prop = format_property("collection_type", row.get("collection_type"), "number")
            if prop:
                properties["collection_type"] = prop
            
            prop = format_property("score", row.get("score"), "number")
            if prop:
                properties["score"] = prop
            
            prop = format_property("rank", row.get("rank"), "number")
            if prop:
                properties["rank"] = prop
            
            prop = format_property("collection_total", row.get("collection_total"), "number")
            if prop:
                properties["collection_total"] = prop
            
            prop = format_property("eps", row.get("eps"), "number")
            if prop:
                properties["eps"] = prop
            
            # Rich text fields
            prop = format_property("name_cn", row.get("name_cn"), "rich_text")
            if prop:
                properties["name_cn"] = prop
            
            prop = format_property("created_at", row.get("created_at"), "rich_text")
            if prop:
                properties["created_at"] = prop
            
            prop = format_property("air_date", row.get("air_date"), "rich_text")
            if prop:
                properties["air_date"] = prop
            
            prop = format_property("all_tags", row.get("all_tags"), "rich_text")
            if prop:
                properties["all_tags"] = prop
            
            # Date field
            prop = format_property("updated_at", row.get("updated_at"), "date")
            if prop:
                properties["updated_at"] = prop
            
            # Insert the row
            response = requests.post(
                "https://api.notion.com/v1/pages",
                headers=headers,
                json={
                    "parent": {"database_id": database_id},
                    "properties": properties
                }
            )
            
            if response.status_code == 200:
                success_count += 1
                if (index + 1) % 10 == 0:
                    logger.info(f"  ✓ Inserted {index + 1}/{len(df_analytics)} rows")
            else:
                error_count += 1
                logger.error(f"  ✗ Failed row {index + 1}: {response.text}")
        
        except Exception as e:
            error_count += 1
            logger.error(f"  ✗ Error on row {index + 1}: {str(e)}")
            continue
    
    logger.info(f"Insertion complete: {success_count} succeeded, {error_count} failed")
    return success_count, error_count


def main():
    """Main execution function"""
    try:
        logger.info("="*60)
        logger.info("Starting Notion Data Ingestion")
        logger.info("="*60)
        
        # Step 1: Create or use existing parent page
        logger.info("\n[Step 1/3] Setting up parent page...")
        parent_page_id = create_page(
            parent_id=notion_config.get("parent_id"),
            notion_token=notion_config.get("notion_token")
        )
        
        # Step 2: Create database
        logger.info("\n[Step 2/3] Creating database...")
        db_id = create_database(
            parent_id=parent_page_id,
            notion_token=notion_config.get("notion_token"),
            database_id=notion_config.get("database_id")  # FIXED: Pass database_id from config
        )
        
        # Step 3: Insert data
        logger.info("\n[Step 3/3] Inserting data...")
        success, errors = insert_data_into_database(
            parent_id=parent_page_id,
            notion_token=notion_config.get("notion_token"),
            database_id=db_id
        )
        
        # Print summary
        print("\n" + "="*60)
        print("INGESTION COMPLETE!")
        print("="*60)
        print(f"Parent Page ID: {parent_page_id}")
        print(f"Database ID: {db_id}")
        print(f"Database URL: https://notion.so/{db_id.replace('-', '')}")
        print(f"Success: {success} rows")
        print(f"Errors: {errors} rows")
        print("="*60)
        
        # Save output
        output_data = {
            "parent_page_id": parent_page_id,
            "database_id": db_id,
            "database_url": f"https://notion.so/{db_id.replace('-', '')}",
            "success_count": success,
            "error_count": errors
        }
        
        with open('notion_output.json', 'w') as f:
            json.dump(output_data, f, indent=2)
        
        logger.info("Saved output to notion_output.json")
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()