import sqlite3
import logging
import argparse

# Levels in the BOLD taxonomy
levels = [
    'kingdom',
    'phylum',
    'class',
    'order',
    'family',
    'subfamily',
    'genus',
    'species',
    'subspecies'
]

# Function to get lowest defined taxon
def get_lowest_defined_taxon(record):
    for i in range(len(levels) - 1, -1, -1):
        level = levels[i]
        if record[level] != 'None':
            return i
    return None

# Function to get taxonid
def get_taxonid(record, max_level, node, conn):
    taxonid = None
    kingdom = record['kingdom']
    for i in range(max_level + 1):
        level = levels[i]
        name = record[level]

        # already seen this taxonomic path in the tree
        if name in node:
            taxonid = node[name]['object']
        else:
            # create node
            taxonid = get_node(kingdom, name, level, taxonid, conn)

            # extend tree
            node[name] = {'children': {}, 'object': taxonid}

        # traverse down the tree
        node = node[name]['children']
    return taxonid

# Function to get node
def get_node(kingdom, name, level, parent, conn):
    cursor = conn.cursor()
    predicates = {'kingdom': kingdom, 'name': name, 'level': level}
    if parent is not None:
        predicates['parent_taxonid'] = parent

    # Either an object or None
    cursor.execute(
        "SELECT taxonid FROM Taxa WHERE kingdom = ? AND name = ? AND level = ? AND (parent_taxonid = ? OR parent_taxonid IS NULL)",
        (kingdom, name, level, parent)
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    else:
        cursor.execute(
            "INSERT INTO Taxa (kingdom, name, level, parent_taxonid) VALUES (?, ?, ?, ?)",
            (kingdom, name, level, parent)
        )
        conn.commit()
        return cursor.lastrowid

# Main function to load taxonomy
def load_taxonomy(db_file, log_level='INFO'):
    # Initialize logger
    log_level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format='%(message)s')
    logger = logging.getLogger('load_taxonomy')

    # Connect to the database
    logger.info(f"Going to connect to database {db_file}")
    conn = sqlite3.connect(db_file)

    # Iterate over all BOLD records to link to the taxa table
    logger.info("Going to link BOLD records to normalized taxa")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Bold")
    records = cursor.fetchall()
    tree = {}  # this is a cache that is gradually filled up
    for record in records:
        logger.info(f"Processing record {record['recordid']}") if record['recordid'] % 1000 == 0 else None

        # Find lowest defined taxon name in BOLD record
        max_level = get_lowest_defined_taxon(record)

        # Find lowest defined taxon object either from cache or DB
        taxonid = get_taxonid(record, max_level, tree, conn)

        # Update the taxon id
        cursor.execute("UPDATE Bold SET taxonid = ? WHERE recordid = ?", (taxonid, record['recordid']))
    conn.commit()
    conn.close()
    logger.info("All records processed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load taxonomy into the database.')
    parser.add_argument('db_file', help='Path to the database file')
    parser.add_argument('log_level', help='Logging level', default='INFO', nargs='?')
    args = parser.parse_args()

    load_taxonomy(args.db_file, args.log_level)