import sqlite3
import json
import requests
import time
import logging
from argparse import ArgumentParser

# Constants
SLEEP = 0.0005  # Sleep time in seconds (500 microseconds)
BASE_URL = 'https://caos.boldsystems.org:443/api/images?processids='
IMAGE_URL = 'https://caos.boldsystems.org:443/api/objects/'

# Process command line arguments
parser = ArgumentParser(description='Check for images')
parser.add_argument('--db', type=str, help='Database file')
parser.add_argument('--tsv', type=str, help='TSV file')
parser.add_argument('--log', type=str, default='INFO', help='Log level')
parser.add_argument('--persist', action='store_true', help='Persist results to database')
args = parser.parse_args()

# Initialize logger
log_level = getattr(logging, args.log.upper(), logging.INFO)
logging.basicConfig(level=log_level, format='%(message)s')
logger = logging.getLogger('assess_criteria')
logger.info("Going to check for images")

# Connect to the database or open TSV file
conn = None
if args.db:
    logger.info(f"Going to connect to database {args.db}")
    conn = sqlite3.connect(args.db)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Bold")
    records = cursor.fetchall()
elif args.tsv:
    logger.info(f"Going to open TSV file {args.tsv}")
    with open(args.tsv, 'r') as file:
        records = [line.strip().split('\t') for line in file]

# Instantiate user agent
session = requests.Session()
logger.info(f"Instantiated user agent {session}")

# Function to process queue
def process_queue(queue):
    process_ids = ','.join(record['processid'] for record in queue)
    wspoint = f"{BASE_URL}{process_ids}"
    logger.debug(f"Attempting {wspoint}")
    response = session.get(wspoint)
    logger.debug(response)

    if response.status_code == 200:
        data = response.json()
        logger.debug(json.dumps(data, indent=2))

        # Iterate over the current queue, check for each record if there is an entry in the JSON response
        for record in queue:
            pid = record['processid']
            rid = record['recordid']
            match = next((item for item in data if item['processid'] == pid), None)
            result = [rid, 'HAS_IMAGE']
            if match:
                result.extend([1, f"{IMAGE_URL}{match['objectid']}"])
            else:
                result.extend([0, ':-('])
            print("\t".join(map(str, result)))
    else:
        response.raise_for_status()

# Process records
queue = []
for record in records:
    if len(queue) == 100:
        process_queue(queue)
        queue = []
        time.sleep(SLEEP)
    queue.append(record)

# Process remaining records
if queue:
    process_queue(queue)

if conn:
    conn.close()