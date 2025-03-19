#!/usr/bin/env python
"""
CSV to PostgreSQL Data Import Script

This script imports data from CSV files into a PostgreSQL database for a Django project.
It supports importing data for Realtors and Listings models.

Usage:
    python upload_data_test.py --model [realtor|listing] --file [csv_file_path]

Example:
    python upload_data_test.py --model realtor --file realtors.csv
    python upload_data_test.py --model listing --file listings.csv
"""

import os
import sys
import csv
import argparse
import logging
import psycopg2
from datetime import datetime
from psycopg2 import sql
from psycopg2.extras import execute_values

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_import.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Database connection parameters from Django settings
DB_PARAMS = {
    'dbname': 'bcredb',
    'user': 'postgres',
    'password': 'rc13081308',
    'host': 'localhost'
}

# CSV field mappings for each model
REALTOR_FIELDS = {
    'name': str,
    'photo': str,  # Path to photo file
    'description': str,
    'phone': str,
    'email': str,
    'is_mvp': lambda x: x.lower() == 'true',  # Convert string to boolean
    'hire_date': lambda x: datetime.strptime(x, '%Y-%m-%d') if x else datetime.now()
}

LISTING_FIELDS = {
    'realtor_id': int,  # Foreign key to realtor
    'title': str,
    'address': str,
    'street': str,
    'district': str,
    'description': str,
    'price': int,
    'bedrooms': int,
    'bathrooms': float,
    'clubhouse': int,
    'sqft': int,
    'estate_size': float,
    'is_published': lambda x: x.lower() == 'true',  # Convert string to boolean
    'list_date': lambda x: datetime.strptime(x, '%Y-%m-%d') if x else datetime.now(),
    'photo_main': str,  # Path to photo file
    'photo_1': str,
    'photo_2': str,
    'photo_3': str,
    'photo_4': str,
    'photo_5': str,
    'photo_6': str
}

# Valid districts from choices.py
VALID_DISTRICTS = [
    "Islands", "Kwai Tsing", "Sai Kung", "Tsuen Wan", "Tuen Mun", 
    "Yuen Long", "Wong Tai Sin", "Sha Tin", "Tai Po", "Kowloon City", 
    "Kwun Tong", "Sham Shui Po", "Yau Tsim Mong", "Central & Western", 
    "Eastern", "Southern", "Wan Chai", "North"
]

def connect_to_db():
    """Establish a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        logger.info("Successfully connected to PostgreSQL database")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL database: {e}")
        sys.exit(1)

def validate_data(model, row_data):
    """Validate data before insertion."""
    errors = []
    
    if model == 'realtor':
        # Validate realtor data
        if not row_data.get('name'):
            errors.append("Name is required")
        if not row_data.get('email'):
            errors.append("Email is required")
        if not row_data.get('phone'):
            errors.append("Phone is required")
    
    elif model == 'listing':
        # Validate listing data
        if not row_data.get('title'):
            errors.append("Title is required")
        if not row_data.get('price'):
            errors.append("Price is required")
        if not row_data.get('realtor_id'):
            errors.append("Realtor ID is required")
        
        # Validate district is in valid choices
        if row_data.get('district') and row_data['district'] not in VALID_DISTRICTS:
            errors.append(f"Invalid district: {row_data['district']}. Must be one of: {', '.join(VALID_DISTRICTS)}")
    
    return errors

def process_csv_file(file_path, model):
    """Process the CSV file and return data ready for database insertion."""
    processed_data = []
    field_map = REALTOR_FIELDS if model == 'realtor' else LISTING_FIELDS
    
    try:
        with open(file_path, 'r', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            
            for i, row in enumerate(reader, start=1):
                row_data = {}
                
                # Process each field according to its type
                for field, type_func in field_map.items():
                    if field in row:
                        try:
                            # Skip empty values for optional fields
                            if row[field] == '' and field not in ['name', 'email', 'phone', 'title', 'price', 'realtor_id']:
                                row_data[field] = None
                            else:
                                row_data[field] = type_func(row[field])
                        except Exception as e:
                            logger.warning(f"Error processing field '{field}' in row {i}: {e}")
                            row_data[field] = None
                
                # Validate the data
                errors = validate_data(model, row_data)
                if errors:
                    logger.warning(f"Validation errors in row {i}: {', '.join(errors)}")
                    continue
                
                processed_data.append(row_data)
                
        logger.info(f"Processed {len(processed_data)} valid records from {file_path}")
        return processed_data
    
    except FileNotFoundError:
        logger.error(f"CSV file not found: {file_path}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error processing CSV file: {e}")
        sys.exit(1)

def insert_realtors(conn, data):
    """Insert realtor data into the database."""
    cursor = conn.cursor()
    
    try:
        # Prepare data for insertion
        columns = list(REALTOR_FIELDS.keys())
        values = []
        
        for row in data:
            row_values = []
            for col in columns:
                row_values.append(row.get(col))
            values.append(tuple(row_values))
        
        # Construct the SQL query
        query = sql.SQL("""
            INSERT INTO realtors_realtor ({})
            VALUES %s
            RETURNING id
        """).format(
            sql.SQL(', ').join(map(sql.Identifier, columns))
        )
        
        # Execute the query
        execute_values(cursor, query, values)
        inserted_ids = [row[0] for row in cursor.fetchall()]
        
        conn.commit()
        logger.info(f"Successfully inserted {len(inserted_ids)} realtors")
        return inserted_ids
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting realtors: {e}")
        return []
    
    finally:
        cursor.close()

def insert_listings(conn, data):
    """Insert listing data into the database."""
    cursor = conn.cursor()
    
    try:
        # Prepare data for insertion
        columns = list(LISTING_FIELDS.keys())
        values = []
        
        for row in data:
            row_values = []
            for col in columns:
                row_values.append(row.get(col))
            values.append(tuple(row_values))
        
        # Construct the SQL query
        query = sql.SQL("""
            INSERT INTO listings_listing ({})
            VALUES %s
            RETURNING id
        """).format(
            sql.SQL(', ').join(map(sql.Identifier, columns))
        )
        
        # Execute the query
        execute_values(cursor, query, values)
        inserted_ids = [row[0] for row in cursor.fetchall()]
        
        conn.commit()
        logger.info(f"Successfully inserted {len(inserted_ids)} listings")
        return inserted_ids
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting listings: {e}")
        return []
    
    finally:
        cursor.close()

def verify_foreign_keys(conn, data, model):
    """Verify that foreign keys exist in the database."""
    if model != 'listing':
        return data
    
    cursor = conn.cursor()
    valid_data = []
    
    try:
        # Get all realtor IDs
        cursor.execute("SELECT id FROM realtors_realtor")
        valid_realtor_ids = {row[0] for row in cursor.fetchall()}
        
        for row in data:
            realtor_id = row.get('realtor_id')
            if realtor_id in valid_realtor_ids:
                valid_data.append(row)
            else:
                logger.warning(f"Skipping listing with invalid realtor_id: {realtor_id}")
        
        return valid_data
    
    except Exception as e:
        logger.error(f"Error verifying foreign keys: {e}")
        return data
    
    finally:
        cursor.close()

def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(description='Import data from CSV to PostgreSQL')
    parser.add_argument('--model', required=True, choices=['realtor', 'listing'],
                        help='Model to import data for (realtor or listing)')
    parser.add_argument('--file', required=True, help='Path to the CSV file')
    
    args = parser.parse_args()
    
    logger.info(f"Starting import for model: {args.model} from file: {args.file}")
    
    # Connect to the database
    conn = connect_to_db()
    
    try:
        # Process the CSV file
        data = process_csv_file(args.file, args.model)
        
        if not data:
            logger.warning("No valid data to import")
            return
        
        # Verify foreign keys for listings
        if args.model == 'listing':
            data = verify_foreign_keys(conn, data, args.model)
            
            if not data:
                logger.warning("No valid listings to import after foreign key verification")
                return
        
        # Insert data into the database
        if args.model == 'realtor':
            inserted_ids = insert_realtors(conn, data)
        else:
            inserted_ids = insert_listings(conn, data)
        
        logger.info(f"Import completed. Inserted {len(inserted_ids)} records.")
    
    finally:
        conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    main()
