"""
Script: clean.py
Description: This script removes intermediate files generated during the pipeline execution.
Input: None
Output: None (removes files from the 'results' directory)
"""

import os
import glob

def clean_results():
    """
    Removes intermediate files from the 'results' directory.
    """
    for file_pattern in ["results/*.tsv", "results/*.ok", "results/*.db"]:
        for file_path in glob.glob(file_pattern):
            os.remove(file_path)

if __name__ == "__main__":
    clean_results()