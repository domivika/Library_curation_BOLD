"""
Script: access_criteria.py
Description: This script assesses a specified criterion for each record in the BOLD data.
Input: 
    - bold_data_tsv: Path to the input BOLD data TSV file.
    - criterion: The criterion to be assessed.
    - image_url: Boolean flag to specify whether the image_url file should be generated.
Output: 
    - output_tsv: Path to the output TSV file containing the assessed criterion.
    - image_url_tsv: Path to the output TSV file containing the image URLs (if specified).
"""

import pandas as pd
import argparse
import logging
import asyncio
from aiohttp import ClientSession, ClientTimeout

# Constants for HAS_IMAGE criterion
BASE_URL = 'https://caos.boldsystems.org:443/api/images?processids='
IMAGE_URL = 'https://caos.boldsystems.org:443/api/objects/'
CHUNK_SIZE = 200  # Increased chunk size for better performance
SLEEP = 0.5  # seconds
MAX_RETRIES = 3  # Maximum number of retries for failed requests
CONCURRENT_REQUESTS = 300  # Limit the number of concurrent requests

async def fetch_images(session, process_ids, retries=0):
    try:
        async with session.get(BASE_URL + process_ids, timeout=ClientTimeout(total=60)) as response:
            if response.status == 200:
                return await response.json()
            else:
                logging.error(f"Failed to fetch images: {response.status} {response.reason}")
                if retries < MAX_RETRIES:
                    logging.info(f"Retrying... ({retries + 1}/{MAX_RETRIES})")
                    await asyncio.sleep(SLEEP)
                    return await fetch_images(session, process_ids, retries + 1)
                else:
                    raise Exception(f"Failed to fetch images after {MAX_RETRIES} retries: {response.status} {response.reason}")
    except asyncio.TimeoutError:
        logging.error(f"Timeout error for process_ids: {process_ids}")
        if retries < MAX_RETRIES:
            logging.info(f"Retrying... ({retries + 1}/{MAX_RETRIES})")
            await asyncio.sleep(SLEEP)
            return await fetch_images(session, process_ids, retries + 1)
        else:
            raise

async def assess_has_image(df, image_url_flag, image_url_tsv):
    results = []
    image_urls = []

    async with ClientSession() as session:
        tasks = []
        for i in range(0, len(df), CHUNK_SIZE):
            chunk = df.iloc[i:i+CHUNK_SIZE]
            process_ids = ','.join(chunk['processid'].astype(str))
            tasks.append(fetch_images(session, process_ids))

            # Limit the number of concurrent requests
            if len(tasks) >= CONCURRENT_REQUESTS:
                responses = await asyncio.gather(*tasks, return_exceptions=True)
                tasks = []
                for response, chunk in zip(responses, [df.iloc[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]):
                    if isinstance(response, Exception):
                        logging.error(f"Error fetching images for chunk: {response}")
                        continue
                    for record in chunk.itertuples():
                        match = next((item for item in response if item['processid'] == record.processid), None)
                        if match:
                            results.append((record.record_id, 1))
                            if image_url_flag:
                                image_urls.append((record.record_id, IMAGE_URL + match['objectid']))
                        else:
                            results.append((record.record_id, 0))

        # Process any remaining tasks
        if tasks:
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            for response, chunk in zip(responses, [df.iloc[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]):
                if isinstance(response, Exception):
                    logging.error(f"Error fetching images for chunk: {response}")
                    continue
                for record in chunk.itertuples():
                    match = next((item for item in response if item['processid'] == record.processid), None)
                    if match:
                        results.append((record.record_id, 1))
                        if image_url_flag:
                            image_urls.append((record.record_id, IMAGE_URL + match['objectid']))
                    else:
                        results.append((record.record_id, 0))

    results_df = pd.DataFrame(results, columns=['record_id', 'HAS_IMAGE'])
    if image_url_flag:
        image_urls_df = pd.DataFrame(image_urls, columns=['record_id', 'image_url'])
        image_urls_df.to_csv(image_url_tsv, sep='\t', index=False)
    return results_df

def access_criteria(bold_data_tsv, criterion, output_tsv, image_url_flag):
    """
    Assesses a specified criterion for each record in the BOLD data.
    """
    logging.basicConfig(filename='logs/access_criteria.log', level=logging.INFO)
    logging.info(f"Assessing criterion: {criterion}")

    try:
        df = pd.read_csv(bold_data_tsv, sep='\t', low_memory=False)
    except FileNotFoundError:
        logging.error(f"File not found: {bold_data_tsv}")
        raise
    except pd.errors.EmptyDataError:
        logging.error(f"Empty data file: {bold_data_tsv}")
        raise
    except Exception as e:
        logging.error(f"Error reading file {bold_data_tsv}: {e}")
        raise

    if criterion == 'HAS_IMAGE':
        image_url_tsv = output_tsv.replace("accessed_HAS_IMAGE.tsv", "image_urls.tsv")
        results_df = asyncio.run(assess_has_image(df, image_url_flag, image_url_tsv))
        results_df.to_csv(output_tsv, sep='\t', index=False)
        return
    elif criterion == 'SPECIES_ID':
        df['SPECIES_ID'] = df['species'].apply(lambda x: 1 if pd.notnull(x) and 'sp.' not in x else 0)
    elif criterion == 'SEQ_QUALITY':
        if 'nuc' in df.columns:
            df['SEQ_QUALITY'] = df['nuc'].apply(lambda x: 1 if isinstance(x, str) and len(x.replace('-', '')) > 500 else 0)
        else:
            logging.error("Column 'nuc' not found in the input file.")
            raise KeyError("Column 'nuc' not found in the input file.")
    elif criterion == 'TYPE_SPECIMEN':
        types = ['holotype', 'lectotype', 'isotype', 'syntype', 'paratype', 'neotype', 'allotype', 'paralectotype', 'hapantotype', 'cotype']
        df['TYPE_SPECIMEN'] = df['taxonomy_notes'].apply(lambda x: 1 if any(t in str(x).lower() for t in types) else 0)
    elif criterion == 'PUBLIC_VOUCHER':
        pos = ['herb', 'museum', 'registered', 'type', 'national', 'CBG', 'INHS', 'deposit', 'harbarium', 'hebarium', 'holot']
        neg = ['DNA', 'e-vouch', 'privat', 'no voucher', 'unvouchered', 'destr', 'lost', 'missing', 'no specimen', 'none', 'not vouchered', 'person', 'Photo Voucher Only', 'not registered']
        df['PUBLIC_VOUCHER'] = df['voucher_type'].apply(lambda x: 1 if any(p in str(x).lower() for p in pos) and not any(n in str(x).lower() for n in neg) else 0)
    elif criterion == 'IDENTIFIER':
        cbg = ['Kate Perez', 'Angela Telfer', 'BOLD ID Engine']
        df['IDENTIFIER'] = df['identified_by'].apply(lambda x: 0 if x in cbg or pd.isnull(x) else 1)
    elif criterion == 'ID_METHOD':
        pos = ['descr', 'det', 'diss', 'exam', 'expert', 'genit', 'identifier', 'key', 'label', 'literature', 'micros', 'mor', 'taxonomic', 'type', 'vou', 'guide', 'flora', 'specimen', 'traditional', 'visual', 'wing', 'logical', 'knowledge', 'photo', 'verified', 'key']
        neg = ['barco', 'BOLD', 'CO1', 'COI', 'COX', 'DNA', 'mole', 'phylo', 'sequ', 'tree', 'bin', 'silva', 'ncbi', 'engine', 'blast', 'genbank', 'genetic', 'its']
        df['ID_METHOD'] = df['identification_method'].apply(lambda x: 1 if any(p in str(x).lower() for p in pos) and not any(n in str(x).lower() for n in neg) else 0)
    elif criterion == 'COLLECTORS':
        df['COLLECTORS'] = df['collectors'].apply(lambda x: 1 if pd.notnull(x) else 0)
    elif criterion == 'COLLECTION_DATE':
        df['COLLECTION_DATE'] = df.apply(lambda x: 1 if pd.notnull(x['collection_date_start']) or pd.notnull(x['collection_date_end']) else 0, axis=1)
    elif criterion == 'COUNTRY':
        df['COUNTRY'] = df['country/ocean'].apply(lambda x: 1 if pd.notnull(x) else 0)
    elif criterion == 'SITE':
        df['SITE'] = df['site'].apply(lambda x: 1 if pd.notnull(x) else 0)
    elif criterion == 'COORD':
        df['COORD'] = df['coord'].apply(lambda x: 1 if pd.notnull(x) else 0)
    elif criterion == 'INSTITUTION':
        neg = ['genbank', 'no voucher', 'personal', 'private', 'research collection of', 'unknown', 'unvouchered']
        df['INSTITUTION'] = df['inst'].apply(lambda x: 0 if any(n in str(x).lower() for n in neg) or pd.isnull(x) else 1)
    elif criterion == 'MUSEUM_ID':
        df['MUSEUM_ID'] = df['museumid'].apply(lambda x: 1 if pd.notnull(x) else 0)

    if 'record_id' not in df.columns:
        logging.error("Column 'record_id' not found in the input file.")
        raise KeyError("Column 'record_id' not found in the input file.")

    df[['record_id', criterion]].to_csv(output_tsv, sep='\t', index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Assess a specified criterion for each record in the BOLD data.")
    parser.add_argument('--bold_data_tsv', required=True, help="Path to the input BOLD data TSV file.")
    parser.add_argument('--criterion', required=True, help="The criterion to be assessed.")
    parser.add_argument('--output_tsv', required=True, help="Path to the output TSV file containing the assessed criterion.")
    parser.add_argument('--image_url', required=False, default=False, action='store_true', help="Flag to specify whether the image_url file should be generated.")
    args = parser.parse_args()

    access_criteria(args.bold_data_tsv, args.criterion, args.output_tsv, args.image_url)