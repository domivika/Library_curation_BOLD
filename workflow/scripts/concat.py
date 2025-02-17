"""
Script: concat.py
Description: This script concatenates multiple TSV files into a single TSV file.
Input: 
    - criteria: List of criteria to determine the input TSV files.
    - output_path: Path to the output concatenated TSV file.
Output: 
    - output_path: Concatenated TSV file.
"""

import pandas as pd
import argparse

def concatenate_tsvs(file_paths, output_path):
    """
    Concatenates multiple TSV files into a single TSV file.
    """
    dfs = [pd.read_csv(file_path, sep='\t') for file_path in file_paths]
    concatenated_df = pd.concat(dfs, axis=1)
    
    # Remove duplicate columns, keeping the first occurrence
    concatenated_df = concatenated_df.loc[:, ~concatenated_df.columns.duplicated()]
    
    concatenated_df.to_csv(output_path, sep='\t', index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Concatenate multiple TSV files into a single TSV file.")
    parser.add_argument('--criteria', required=True, help="Criteria to determine the input TSV files.")
    parser.add_argument('--output_path', required=True, help="Path to the output concatenated TSV file.")
    args = parser.parse_args()

    criteria = args.criteria.split()
    file_paths = [f"results/accessed_{criterion}.tsv" for criterion in criteria]
    concatenate_tsvs(file_paths, args.output_path)