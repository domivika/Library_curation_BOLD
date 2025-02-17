import csv
import argparse

def filter_tsv(input_file, temp_file):
    with open(input_file, 'r', encoding='utf-8') as infile, open(temp_file, 'w', encoding='utf-8', newline='') as outfile:
        reader = csv.DictReader(infile, delimiter='\t')
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter='\t')
        
        writer.writeheader()
        for row in reader:
            if row['ranking']:  # Check if the ranking field is not empty
                # Ensure the row contains only the expected fieldnames
                filtered_row = {key: row[key] for key in fieldnames}
                writer.writerow(filtered_row)

def filter_by_bin_uri(temp_file, output_file):
    records = {}
    total_records = 0
    removed_records = 0
    
    # Read the temp file and group records by bin_uri
    with open(temp_file, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile, delimiter='\t')
        for row in reader:
            total_records += 1
            bin_uri = row['bin_uri']
            ranking = int(row['ranking']) if row['ranking'] else float('inf')
            
            if bin_uri not in records:
                records[bin_uri] = row
            else:
                current_ranking = int(records[bin_uri]['ranking']) if records[bin_uri]['ranking'] else float('inf')
                if ranking < current_ranking:
                    records[bin_uri] = row
    
    # Calculate the number of removed records
    removed_records = total_records - len(records)
    
    # Write the filtered records to the output file
    with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        fieldnames = reader.fieldnames[:-15]  # Remove the last 15 columns
        writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter='\t')
        writer.writeheader()
        for record in records.values():
            filtered_record = {key: (record[key] if record[key] and record[key] != 'None' else '') for key in fieldnames}
            writer.writerow(filtered_record)
    
    print(f'Total records: {total_records}')
    print(f'Removed records: {removed_records}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Filter TSV file to remove rows with empty ranking field and keep only one record with the lowest ranking score for each unique bin_uri.')
    parser.add_argument('input_file', help='Path to the input TSV file')
    parser.add_argument('output_file', help='Path to the output TSV file')
    args = parser.parse_args()
    
    temp_file = 'temp_filtered.tsv'
    filter_tsv(args.input_file, temp_file)
    filter_by_bin_uri(temp_file, args.output_file)