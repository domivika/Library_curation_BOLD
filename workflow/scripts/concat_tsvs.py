import sys

def concat_tsvs(infiles):
    pk = 1
    for infile in infiles:
        with open(infile, 'r') as fh:
            for line in fh:
                # Write the line prefixed with the primary key and a tab
                sys.stdout.write(f"{pk}\t{line}")
                pk += 1

if __name__ == "__main__":
    infiles = sys.argv[1:]
    concat_tsvs(infiles)