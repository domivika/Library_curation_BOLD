import pandas as pd
import argparse

def ranking_score(db_file, criteria_file, output_path):
    """
    Substitutes the criteria columns, calculates the ranking score, and generates a final output file.
    """
    # Define the chunk size for processing
    chunk_size = 10000

    # Open the output file for writing
    with open(output_path, 'w') as output_file:
        # Write the header to the output file
        header_written = False

        # Process the concatenated TSV file in chunks
        for concatenated_chunk in pd.read_csv(db_file, sep='\t', chunksize=chunk_size, low_memory=False):
            # Process the criteria file in chunks
            for criteria_chunk in pd.read_csv(criteria_file, sep='\t', chunksize=chunk_size, low_memory=False):
                temp_merged_df = pd.merge(criteria_chunk, concatenated_chunk, on='record_id', how='left', suffixes=('', '_new'))

                # Substitute the criteria columns with the new values
                criteria_columns = concatenated_chunk.columns.tolist()[1:]  # Exclude 'record_id'
                for column in criteria_columns:
                    temp_merged_df[column] = temp_merged_df[f"{column}_new"]
                    temp_merged_df.drop(columns=[f"{column}_new"], inplace=True)

                # Apply the ranking system
                def calculate_ranking(row):
                    if row.get('SPECIES_ID') == 1:
                        if row.get('TYPE_SPECIMEN') == 1:
                            return 1
                        elif (row.get('SEQ_QUALITY') == 1 and row.get('HAS_IMAGE') == 1 and row.get('COLLECTORS') == 1 and
                              row.get('COLLECTION_DATE') == 1 and row.get('COUNTRY') == 1 and row.get('SITE') == 1 and row.get('COORD') == 1 and
                              row.get('IDENTIFIER') == 1 and (row.get('ID_METHOD') == 1 or row.get('INSTITUTION') == 1) and
                              (row.get('PUBLIC_VOUCHER') == 1 or row.get('MUSEUM_ID') == 1)):
                            return 2
                        elif (row.get('SEQ_QUALITY') == 1 and row.get('HAS_IMAGE') == 1 and row.get('COUNTRY') == 1 and
                              (row.get('IDENTIFIER') == 1 or row.get('ID_METHOD') == 1) and
                              (row.get('INSTITUTION') == 1 or row.get('PUBLIC_VOUCHER') == 1 or row.get('MUSEUM_ID') == 1)):
                            return 3
                        elif row.get('SEQ_QUALITY') == 1 and row.get('HAS_IMAGE') == 1 and row.get('COUNTRY') == 1:
                            return 4
                        elif row.get('SEQ_QUALITY') == 1 and row.get('HAS_IMAGE') == 1:
                            return 5
                        elif row.get('SEQ_QUALITY') == 1:
                            return 6
                    return None

                temp_merged_df['ranking'] = temp_merged_df.apply(calculate_ranking, axis=1)
                temp_merged_df['ranking'] = temp_merged_df['ranking'].fillna(0).astype('int')  # Fill NaN values with 0 and convert to integer

                # Write the chunk to the output file
                if not header_written:
                    temp_merged_df.to_csv(output_file, sep='\t', index=False, mode='a')
                    header_written = True
                else:
                    temp_merged_df.to_csv(output_file, sep='\t', index=False, mode='a', header=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Substitute the criteria columns, calculate the ranking score, and generate a final output file.")
    parser.add_argument('--db_file', required=True, help="Path to the input TSV file containing the concatenated records.")
    parser.add_argument('--criteria_file', required=True, help="Path to the TSV file containing the criteria.")
    parser.add_argument('--output_path', required=True, help="Path to the output TSV file.")
    args = parser.parse_args()

    ranking_score(args.db_file, args.criteria_file, args.output_path)
