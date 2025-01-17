import sqlite3
import logging
import csv
from argparse import ArgumentParser
import importlib

# Process command line arguments
parser = ArgumentParser(description='Assess criteria')
parser.add_argument('--db', type=str, help='Database file')
parser.add_argument('--tsv', type=str, help='TSV file')
parser.add_argument('--log', type=str, default='INFO', help='Log level')
parser.add_argument('--criteria', nargs='+', help='Criteria to assess')
parser.add_argument('--persist', action='store_true', help='Persist results to database')
args = parser.parse_args()

# Initialize logger
log_level = getattr(logging, args.log.upper(), logging.INFO)
logging.basicConfig(level=log_level, format='%(message)s')
logger = logging.getLogger('assess_criteria')
logger.info(f"Going to assess criteria: {args.criteria}")

# Connect to the database or open TSV file
conn = None
if args.db:
    logger.info(f"Going to connect to database {args.db}")
    conn = sqlite3.connect(args.db)
elif args.tsv:
    logger.info(f"Going to open TSV file {args.tsv}")

# Function to dynamically load criteria implementations
def load_criterion(name):
    try:
        module = importlib.import_module(f"criteria.{name.lower()}")
        return getattr(module, 'assess')
    except (ModuleNotFoundError, AttributeError) as e:
        logger.error(f"Error loading criterion {name}: {e}")
        return None

# Create map of loaded criteria
criteria_impls = {c: load_criterion(c) for c in args.criteria if load_criterion(c) is not None}

# Function to process records and assess criteria
def process_records(records, persist):
    for record in records:
        logger.info(f"Processing record {record['recordid']}") if int(record['recordid']) % 10000 == 0 else None

        # Iterate over loaded criteria implementations
        for name, impl in criteria_impls.items():
            # Handler function to process assessment results
            def handler(status, notes, i):
                if persist:
                    # Persist to database (implementation needed)
                    pass
                else:
                    # Print results to stdout
                    cid = name  # Use the criterion name as ID
                    rid = int(record['recordid']) - i
                    print(f"{rid}\t{cid}\t{status}\t{notes}")

            # Do the assessment
            impl(record, handler)

# Prepare and process records
if args.db:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Bold")
    records = cursor.fetchall()
    process_records(records, args.persist)
elif args.tsv:
    with open(args.tsv, mode='r') as file:
        reader = csv.DictReader(file, delimiter='\t')
        records = [row for row in reader]
        process_records(records, args.persist)

if conn:
    conn.close()