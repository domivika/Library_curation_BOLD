import sqlite3
import csv
import logging
import os
import argparse

# Function to load data into the SQLite database
def load_bcdm(bold_tsv, db_file, schema_sql, overwrite=False, table='bold', log_level='INFO'):
    # Initialize logger
    log_level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format='%(message)s')
    logger = logging.getLogger('load_bcdm')

    # Create the SQLite database - delete old if specified
    if overwrite and os.path.exists(db_file):
        os.remove(db_file)
    elif os.path.exists(db_file):
        logger.error('Database exists and overwrite not specified')
        return

    logger.info(f"Going to create database {db_file} with schema {schema_sql}")
    with open(schema_sql, 'r') as schema_file:
        schema_sql_content = schema_file.read()

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.executescript(schema_sql_content)
    conn.commit()

    # Connect to the BOLD TSV as binary, with UTF-8
    logger.info(f"Going to read BOLD TSV dump {bold_tsv}")
    with open(bold_tsv, 'r', encoding='utf-8') as tsv_file:
        reader = csv.DictReader(tsv_file, delimiter='\t')
        columns = ['recordid'] + reader.fieldnames
        placeholders = ', '.join('?' * len(columns))
        insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        cursor = conn.cursor()

        # Insert rows
        for pk, row in enumerate(reader, start=1):
            values = [pk] + [row[col] for col in reader.fieldnames]
            cursor.execute(insert_sql, values)
            if pk % 10000 == 0:
                logger.info(f"Processed record {pk}")

    conn.commit()
    conn.close()
    logger.info("All records processed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load BCDM data into SQLite database.')
    parser.add_argument('bold_tsv', help='Path to the BOLD TSV file')
    parser.add_argument('db_file', help='Path to the SQLite database file')
    parser.add_argument('schema_sql', help='Path to the SQL schema file')
    parser.add_argument('--overwrite', action='store_true', help='Overwrite the existing database file')
    parser.add_argument('--table', default='bold', help='Name of the table to insert data into')
    parser.add_argument('--log', default='INFO', help='Logging level')

    args = parser.parse_args()

    load_bcdm(args.bold_tsv, args.db_file, args.schema_sql, args.overwrite, args.table, args.log)
