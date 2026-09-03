#-- import modules --#
import io
import os
import sys
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from datetime import datetime

#-------------------------------------------------------
# classes
#-------------------------------------------------------
class SampleStats:
    def __init__(self, sample_id, rep_id, num_total_reads, num_qced_reads, num_align_reads, num_dedup_reads):
        self.sample_id       = sample_id
        self.rep_id          = rep_id
        self.num_total_reads = num_total_reads
        self.num_qced_reads  = num_qced_reads
        self.num_align_reads = num_align_reads
        self.num_dedup_reads = num_dedup_reads

    @property
    def pct_qced_reads(self):
        if self.num_total_reads == 0:
            return 0.0
        return round((self.num_qced_reads / self.num_total_reads) * 100, 2)

    @property
    def pct_align_reads(self):
        if self.num_total_reads == 0:
            return 0.0
        return round((self.num_align_reads / self.num_total_reads) * 100, 2)

    @property
    def pct_dedup_reads(self):
        if self.num_total_reads == 0:
            return 0.0
        return round((self.num_dedup_reads / self.num_total_reads) * 100, 2)

class ParseStats:
    def __init__(self, sample_id, rep_id, fastp_stats, align_stats, dedup_stats):
        self.sample_id   = sample_id
        self.rep_id      = rep_id
        self.fastp_stats = fastp_stats
        self.align_stats = align_stats
        self.dedup_stats = dedup_stats

    def parse_fastp_stats(self):
        with open(self.fastp_stats, 'r') as f:
            for line in f:
                if line.startswith("Read1 before filtering"):
                    next_line = next(f).strip()
                    num_total_reads = int(next_line.split(":")[1].strip())
                elif line.startswith("Read1 after filtering"):
                    next_line = next(f).strip()
                    num_qced_reads = int(next_line.split(":")[1].strip())
                    break
        return num_total_reads, num_qced_reads

    def parse_align_stats(self):
        with open(self.align_stats, 'r') as f:
            for line in f:
                if line.strip().endswith("read1"):
                    num_align_reads = int(line.split("+")[0].strip())
                    break
        return num_align_reads

    def parse_dedup_stats(self):
        with open(self.dedup_stats, 'r') as f:
            for line in f:
                if line.strip().endswith("read1"):
                    num_dedup_reads = int(line.split("+")[0].strip())
                    break
        return num_dedup_reads

    def get_stats(self):
        num_total_reads, num_qced_reads = self.parse_fastp_stats()
        num_align_reads = self.parse_align_stats()
        num_dedup_reads = self.parse_dedup_stats()
        return SampleStats(self.sample_id,
                           self.rep_id,
                           num_total_reads, 
                           num_qced_reads,
                           num_align_reads,
                           num_dedup_reads)

#-------------------------------------------------------
# main execution
#-------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Summerise all the stats", allow_abbrev = False)
    parser.add_argument("--sample_ids",    type = str, required = True,       help = "list of sample IDs")
    parser.add_argument("--rep_ids",       type = str, required = True,       help = "list of replicate IDs")
    parser.add_argument("--fastp_stats",   type = str, required = True,       help = "list of fastp stats files")
    parser.add_argument("--align_stats",   type = str, required = True,       help = "list of alignment stats files")
    parser.add_argument("--dedup_stats",   type = str, required = True,       help = "list of deduplication stats files")
    parser.add_argument("--output_dir",    type = str, default = os.getcwd(), help = "output directory")
    parser.add_argument("--output_prefix", type = str, required = True,       help = "output prefix")

    args, unknown = parser.parse_known_args()

    if unknown:
        print(f"Error: Unrecognized arguments: {' '.join(unknown)}", file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    # -- creating outputs -- #
    output_stats = f"{args.output_prefix}.final_stats.tsv"
    if os.path.exists(output_stats):
        os.remove(output_stats)

    output_num_plot = f"{args.output_prefix}.num_reads_barplot.png"
    if os.path.exists(output_num_plot):
        os.remove(output_num_plot)

    output_pct_plot = f"{args.output_prefix}.pct_reads_barplot.png"
    if os.path.exists(output_pct_plot):
        os.remove(output_pct_plot)

    # -- processing -- #
    list_sample_ids  = args.sample_ids.split(",")
    list_rep_ids     = args.rep_ids.split(",")
    list_fastp_stats = args.fastp_stats.split(",")
    list_align_stats = args.align_stats.split(",")
    list_dedup_stats = args.dedup_stats.split(",")

    rows = []
    for sample_id, rep_id, fastp_stats, align_stats, dedup_stats \
        in zip(list_sample_ids, list_rep_ids, list_fastp_stats, list_align_stats, list_dedup_stats):

        obj_parser = ParseStats(sample_id, rep_id, fastp_stats, align_stats, dedup_stats)
        obj_sample = obj_parser.get_stats()

        row = [ 
            sample_id,
            rep_id,
            obj_sample.num_total_reads,
            obj_sample.num_qced_reads,
            obj_sample.pct_qced_reads,
            obj_sample.num_align_reads,
            obj_sample.pct_align_reads,
            obj_sample.num_dedup_reads,
            obj_sample.pct_dedup_reads
        ]

        rows.append(row)

    columns = [ 
        "Sample",
        "Replicate",
        "Num_Raw_Reads",
        "Num_QCed_Reads",
        "Pct_QCed_Reads",
        "Num_Align_Reads",
        "Pct_Align_Reads",
        "Num_Dedup_Reads",
        "Pct_Dedup_Reads"
    ]    

    df_stats = pd.DataFrame(rows, columns = columns)
    df_stats.to_csv(output_stats, sep = "\t", index = False)

    # -- plotting -- #
    df_stats_nums = df_stats[["Num_Raw_Reads", "Num_QCed_Reads", "Num_Align_Reads", "Num_Dedup_Reads"]]
    df_stats_nums.index = df_stats["Sample"] + "_" + df_stats["Replicate"]

    num_colors = ["tomato", "yellowgreen", "royalblue", "orange"]
    ax = df_stats_nums.plot(kind = "bar", figsize = (8, 6), width = 0.3, color = num_colors)
    ax.set_yscale("log", base = 10)
    ax.legend(loc="center left", bbox_to_anchor = (1.0, 0.5), ncol = 1)
    plt.ylabel("Number of Reads (log10)")
    plt.savefig(output_num_plot, dpi = 300, bbox_inches = "tight")

    df_stats_pct = df_stats[["Pct_QCed_Reads", "Pct_Align_Reads", "Pct_Dedup_Reads"]]
    df_stats_pct.index = df_stats["Sample"] + "_" + df_stats["Replicate"]

    pct_colors = ["yellowgreen", "royalblue", "orange"]
    ax = df_stats_pct.plot(kind = "bar", figsize = (6, 6), width = 0.3, color = pct_colors)
    ax.legend(loc="center left", bbox_to_anchor = (1.0, 0.5), ncol = 1)
    plt.ylabel("Percentage of Reads")
    plt.savefig(output_pct_plot, dpi = 300, bbox_inches = "tight")
