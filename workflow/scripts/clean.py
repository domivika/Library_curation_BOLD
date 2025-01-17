import os
import glob

# Remove all .tsv, .ok, and .db files in the results directory
for file_pattern in ["results/*.tsv", "results/*.ok", "results/*.db"]:
    for file_path in glob.glob(file_pattern):
        os.remove(file_path)