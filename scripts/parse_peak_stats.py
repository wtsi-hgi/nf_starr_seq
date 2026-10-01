#-- import modules --#
import io
import os
import sys
import argparse
import gc
import numpy as np
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from datetime import datetime

#-------------------------------------------------------
# classes
#-------------------------------------------------------
class PeakStats:
    def __init__(self, sample_id, rep_id, num_raw_peaks, num_filtered_peaks, peak_mean_cov, peak_median_cov, peak_covs):
        self.sample_id          = sample_id
        self.rep_id             = rep_id
        self.num_raw_peaks      = num_raw_peaks
        self.num_filtered_peaks = num_filtered_peaks
        self.peak_mean_cov      = peak_mean_cov
        self.peak_median_cov    = peak_median_cov
        self.peak_covs          = peak_covs

class ParseStats:
    def __init__(self, sample_id, rep_id, raw_peaks, filtered_peaks):
        self.sample_id      = sample_id
        self.rep_id         = rep_id
        self.raw_peaks      = raw_peaks
        self.filtered_peaks = filtered_peaks

    def parse_peaks(self):
        df = pl.read_csv(self.raw_peaks, separator = "\t", has_header = True, comment_prefix = "#", columns = ["pileup"], skip_rows = 20)
        num_raw_peaks   = df.height
        peak_mean_cov   = df["pileup"].mean()
        peak_median_cov = df["pileup"].median()
        peak_covs       = df["pileup"]

        num_filtered_peaks = ( pl.scan_csv(self.filtered_peaks, separator = "\t", has_header = False)
                                 .select(pl.len())
                                 .collect()
                                 .item() )

        return PeakStats(self.sample_id,
                         self.rep_id,
                         num_raw_peaks,
                         num_filtered_peaks,
                         peak_mean_cov,
                         peak_median_cov,
                         peak_covs)

#-------------------------------------------------------
# main execution
#-------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Summerise all the stats")
    parser.add_argument("--sample_ids",     type = str, required = True,       help = "list of sample IDs")
    parser.add_argument("--rep_ids",        type = str, required = True,       help = "list of replicate IDs")
    parser.add_argument("--raw_peaks",      type = str, required = True,       help = "list of raw peak xls files")
    parser.add_argument("--filtered_peaks", type = str, required = True,       help = "list of filtered peak files (against blacklist)")
    parser.add_argument("--output_dir",     type = str, default = os.getcwd(), help = "output directory")
    parser.add_argument("--output_prefix",  type = str, required = True,       help = "output prefix")

    args, unknown = parser.parse_known_args()

    if unknown:
        print(f"Error: Unrecognized arguments: {' '.join(unknown)}", file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    # -- creating outputs -- #
    output_stats = f"{args.output_prefix}.peak_stats.tsv"
    if os.path.exists(output_stats):
        os.remove(output_stats)

    output_cov_plot = f"{args.output_prefix}.peak_cov.boxplot.png"
    if os.path.exists(output_cov_plot):
        os.remove(output_cov_plot)

    # -- processing -- #
    list_sample_ids     = args.sample_ids.split(",")
    list_rep_ids        = args.rep_ids.split(",")
    list_raw_peaks      = args.raw_peaks.split(",")
    list_filtered_peaks = args.filtered_peaks.split(",")

    list_rows = []
    dict_peak_covs = {}
    for sample_id, rep_id, raw_peaks, filtered_peaks \
        in zip(list_sample_ids, list_rep_ids, list_raw_peaks, list_filtered_peaks):

        obj_parser = ParseStats(sample_id, rep_id, raw_peaks, filtered_peaks)
        obj_peak   = obj_parser.parse_peaks()

        row = [
            sample_id,
            rep_id,
            obj_peak.num_raw_peaks,
            obj_peak.num_filtered_peaks,
            obj_peak.peak_mean_cov,
            obj_peak.peak_median_cov
        ]

        list_rows.append(row)
        dict_peak_covs[f"{sample_id}_{rep_id}"] = obj_peak.peak_covs

    columns = [ 
        "Sample",
        "Replicate",
        "Num_Raw_Peaks",
        "Num_Filtered_Peaks",
        "Peak_Mean_Cov",
        "Peak_Median_Cov"
    ]    

    df_stats = pd.DataFrame(list_rows, columns = columns)
    df_stats.to_csv(output_stats, sep = "\t", index = False)    
    n_rows = len(df_stats)
    figsize = (20, 1 * n_rows)

    # -- plotting -- #
    fig, ax = plt.subplots(figsize = figsize)

    ax.boxplot(
        [ cov.to_numpy() for cov in dict_peak_covs.values() ],
        labels = list(dict_peak_covs.keys()),
        vert = False,
        patch_artist = True,
        boxprops = dict(facecolor = "ivory"),
        medianprops = dict(color="red", linewidth = 2),
        flierprops = dict(
            marker = "o",
            markerfacecolor = "red",
            markeredgecolor = "red",
            markersize = 5,
            alpha = 0.7
        )
    )
                  
    ax.tick_params(axis = "x", labelsize = 20)
    ax.tick_params(axis = "y", labelsize = 20)
    ax.set_xlabel("Peak Coverage", fontsize = 24)

    fig.tight_layout()
    fig.savefig(output_cov_plot, dpi = 300, bbox_inches = "tight")
    plt.close(fig)
