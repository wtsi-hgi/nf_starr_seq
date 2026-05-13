#-- import modules --#
import io
import os
import sys
import argparse
import re
import gc
import subprocess
import polars as pl
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import islice
from collections import Counter

from sequence_utils import (
    pigz_open,
    fastq_iter_se,
    extract_sequence,
    reverse_complement
)

#-- functions --#
def process_se_read(read: tuple) -> list:
    """
    Get the target sequence by the flanking sequences
    Parameters:
        -- read: tuple (header, sequence, quality) for read
    Returns:
        -- tuple: (read, barcode) 
    """
    read_seq = read[1]

    target_seq = extract_sequence(read_seq, args.target_up, args.target_down, args.max_mismatches)
    if (target_seq in {"upstream not found", "downstream not found"}):
        read_seq_rc = reverse_complement(read_seq)
        target_seq = extract_sequence(read_seq_rc, args.target_up, args.target_down, args.max_mismatches)

    return target_seq

def batch_process_se_reads(batch_reads: list) -> list:
    """
    Process a batch of reads to extract targets.
    Parameters:
        -- batch_reads
    Returns:
        -- list of tuples
    """
    results = []
    for read in batch_reads:
        result = process_se_read(read)
        if result:
            results.append(result)
    return results

def function_processpool_se(args):
    """
    Wrapper function for process pool as ProcessPoolExecutor expects a function rather than returned results.
    """
    return batch_process_se_reads(args)

def process_se_reads_in_chunks(path_read):
    """
    Read single-end FASTQ file in chunks and process reads in parallel
    Parameters:
        -- path_read: path to single-end FASTQ file
    Yields:
        -- DataFrame: barcode
    """
    fh_read = io.TextIOWrapper(pigz_open(path_read).stdout) if path_read.endswith(".gz") else open(path_read)
    read_iter = fastq_iter_se(fh_read)

    with ProcessPoolExecutor(max_workers = args.threads) as executor:
        while True:
            read_chunk = list(islice(read_iter, args.chunk_size))
            if not read_chunk:
                break

            batch_size = min(args.chunk_size, 20000)
            read_batches = [
                read_chunk[i:i+batch_size]
                for i in range(0, len(read_chunk), batch_size)
            ]

            list_targets = []
            futures = [ executor.submit(function_processpool_se, batch) for batch in read_batches ]
            for future in as_completed(futures):
                batch_result = future.result()
                list_targets.append(pl.DataFrame(batch_result, schema = ["target_seq"], orient = "row"))

                # -- free memory -- #
                del batch_result
                gc.collect()

            df_yield = ( pl.concat(list_targets, how = "vertical")
                           .group_by("target_seq")
                           .agg(pl.len().alias("count")) )

            # -- free memory -- #
            del read_chunk, read_batches, futures, list_targets
            gc.collect()

            yield df_yield
    fh_read.close()

#-- main execution --#
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Extract targets by flanking sequences from FASTQ files.", allow_abbrev = False)
    parser.add_argument("--type",             type = str, required = True,       help = "Type of reads [se, pe]",    choices = ["se", "pe"])
    parser.add_argument("--reads",            type = str, required = True,       help = "FASTQ file for SE reads or comma-separated FASTQ files for PE reads")
    parser.add_argument("--target_up",        type = str, required = True,       help = "Upstream flank sequence of target in the read")
    parser.add_argument("--target_down",      type = str, required = True,       help = "Downstream flank sequence of target in the read")
    parser.add_argument("--target_len",       type = int, required = True,       help = "Length of the target sequence")
    parser.add_argument("--max_mismatches",   type = int, default = 2,           help = "Max mismatches allowed in up/down matches")    
    parser.add_argument("--min_cov",          type = int, default = 2,           help = "Minimum coverage for target searching")
    parser.add_argument("--output_dir",       type = str, default = os.getcwd(), help = "output directory")
    parser.add_argument("--output_prefix",    type = str, required = True,       help = "output prefix")
    parser.add_argument("--chunk_size",       type = int, default = 100000,      help = "Chunk size for processing reads")
    parser.add_argument("--threads",          type = int, default = 40,          help = "Number of threads")

    args, unknown = parser.parse_known_args()

    if unknown:
        print(f"Error: Unrecognized arguments: {' '.join(unknown)}", file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    # -- checking arguments -- #
    if args.type == "se":
        path_read = args.reads
        if not os.path.exists(path_read):
            print(f"Error: File {path_read} does not exist!", file=sys.stderr)
            sys.exit(1)
    else:
        paths = args.reads.split(",")
        if len(paths) != 2:
            print(f"Error: For paired-end reads, please provide exactly two FASTQ files separated by a comma!", file=sys.stderr)
            sys.exit(1)
        path_read1, path_read2 = paths
        if not os.path.exists(path_read1):
            print(f"Error: File {path_read1} does not exist!", file=sys.stderr)
            sys.exit(1)
        if not os.path.exists(path_read2):
            print(f"Error: File {path_read2} does not exist!", file=sys.stderr)
            sys.exit(1)

    # -- creating outputs -- #
    target_out = f"{args.output_prefix}.target_counts.tsv"
    if os.path.exists(target_out):
        os.remove(target_out)

    stats_out = f"{args.output_prefix}.target_counts.stats.tsv"
    if os.path.exists(stats_out):
        os.remove(stats_out)

    # -- processing -- #
    if args.type == "se":
        path_input = path_read
    else:
        path_input = path_read1

    print(f"Detecting target sequences, please wait...", flush=True)
    list_results = []
    for i, chunk_result in enumerate(process_se_reads_in_chunks(path_input)):
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} --> Processed chunk {i+1} with {args.chunk_size} reads", flush=True)
        if not chunk_result.is_empty():
            list_results.append(chunk_result)
    print(f"Finished processing all the reads.", flush=True)

    # -- clean and format the extracted sequences from reads -- #
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Generating target results, please wait ...", flush = True)
    list_results_filtered = [df for df in list_results if df.height > 0]
    if list_results_filtered:
        df_target = pl.concat(list_results_filtered, how = "vertical")
        df_target_counts = ( df_target.group_by("target_seq")
                                      .agg(pl.sum("count").alias("count")) )
    else:
        with open(target_out, "w") as f:
            f.write("no target found in the reads, please check your flanking sequences or template!\n")
        exit(0)

    count_processed_reads = df_target_counts["count"].sum()
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Filtering target results, please wait ...", flush = True)

    # -- upstream not found -- #
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} --> Filtering against upstream match, please wait ...", flush=True)
    mask = df_target_counts["target_seq"] == "upstream not found"
    count_up_notfound = df_target_counts.filter(mask)["count"].sum()
    df_target_counts = df_target_counts.filter(~mask)

    # -- downstream not found -- #
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} --> Filtering against downstream match, please wait ...", flush=True)
    mask = df_target_counts["target_seq"] == "downstream not found"
    count_down_notfound = df_target_counts.filter(mask)["count"].sum()
    df_target_counts = df_target_counts.filter(~mask)

    # -- target length check -- #
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} --> Filtering against target length, please wait ...", flush=True)
    mask = df_target_counts["target_seq"].str.len_chars() != args.target_len
    count_length_mismatch = df_target_counts.filter(mask)["count"].sum()
    df_target_counts = df_target_counts.filter(~mask)

    # -- target coverage check -- #
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} --> Filtering against target coverage, please wait ...", flush=True)
    mask = df_target_counts["count"] < args.min_cov
    count_low_cov = df_target_counts.filter(mask)["count"].sum()
    df_target_counts = df_target_counts.filter(~mask)

    # -- remaining records -- #
    df_target_counts = df_target_counts.sort("target_seq")
    count_effective_targets = df_target_counts["count"].sum()

    print(f"Creating output files, please wait...", flush=True)
    df_target_counts.write_csv(target_out, separator = "\t")

    with open(stats_out, "w") as f:
        f.write(f"Total reads processed: {count_processed_reads}\n")
        f.write(f"Total reads with upstream not found: {count_up_notfound}\n")
        f.write(f"Total reads with downstream not found: {count_down_notfound}\n")
        f.write(f"Total reads with target length inconsistent: {count_length_mismatch}\n")
        f.write(f"Total reads with target coverage < {args.min_barcov}: {count_low_cov}\n")
        f.write(f"Total effective reads: {count_effective_targets}\n")
