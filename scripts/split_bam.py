#-- import modules --#
import io
import os
import sys
import argparse
import pysam
import random
from datetime import datetime

#-------------------------------------------------------
# basic function
#-------------------------------------------------------
def split_name_sorted_bam(bam: str, subset1: str, subset2: str, read_type: str,seed: int, threads: int) -> None:
    rng = random.Random(seed)

    bam = pysam.AlignmentFile(bam, "rb", threads = threads)
    bam_subset1 = pysam.AlignmentFile(subset1, "wb", header = bam.header, threads = threads)
    bam_subset2 = pysam.AlignmentFile(subset2, "wb", header = bam.header, threads = threads)

    if read_type == "se":
        for read in bam:
            write = bam_subset1.write if rng.random() < 0.5 else bam_subset2.write
            write(read)
    else:
        current_name = None
        group = []

        for read in bam:
            if current_name is None:
                current_name = read.query_name

            if read.query_name != current_name:
                write = bam_subset1.write if rng.random() < 0.5 else bam_subset2.write
                for r in group:
                    write(r)

                group.clear()
                current_name = read.query_name

            group.append(read)

        if group:
            write = bam_subset1.write if rng.random() < 0.5 else bam_subset2.write
            for r in group:
                write(r)

    bam.close()
    bam_subset1.close()
    bam_subset2.close()

#-------------------------------------------------------
# main execution
#-------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Split a BAM file into two sub BAM files", allow_abbrev = False)
    parser.add_argument("--bam",           type = str, required = True,       help = "Input BAM file")
    parser.add_argument("--read_type",     type = str, default = "pe",        help = "sequence read type (se or pe)", choices = ['se', 'pe'])
    parser.add_argument("--output_dir",    type = str, default = os.getcwd(), help = "output directory")
    parser.add_argument("--output_prefix", type = str, required = True,       help = "output prefix")
    parser.add_argument("--threads",       type = int, default = 16,          help = "number of threads")
    parser.add_argument("--seed",          type = int, default = 16,          help = "random seed for reproducibility")

    args, unknown = parser.parse_known_args()

    if unknown:
        print(f"Error: Unrecognized arguments: {' '.join(unknown)}", file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    # -- creating outputs -- #
    subset1 = f"{args.output_prefix}.subset1.bam"
    if os.path.exists(subset1):
        os.remove(subset1)
    
    subset2 = f"{args.output_prefix}.subset2.bam"
    if os.path.exists(subset2):
        os.remove(subset2)

    #-- processing --#
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Splitting BAM file, please wait...", flush=True)
    split_name_sorted_bam(args.bam, subset1, subset2, args.read_type, args.seed, args.threads)
