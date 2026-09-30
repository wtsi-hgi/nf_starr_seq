#!/usr/bin/env Rscript
quiet_library <- function(pkg) { suppressMessages(suppressWarnings(library(pkg, character.only = TRUE))) }
packages <- c("tidyverse", "data.table", "vroom", "htmltools", "reactable", "optparse", "sparkline", "glue", "gtools")
invisible(lapply(packages, quiet_library))

option_list <- list(make_option("--rscript_dir",          type = "character", help = "directory path of R scripts",                   default = NULL),
                    make_option("--lib_type",             type = "character", help = "library type",                                  default = NULL),
                    make_option("--basic_stats",          type = "character", help = "file of basic stats",                           default = NULL),
                    make_option("--basic_plots",          type = "character", help = "figures of basic stats",                        default = NULL),
                    make_option("--output_dir",           type = "character", help = "output directory",                              default = getwd()),
                    make_option("--prefix",               type = "character", help = "output prefix",                                 default = "sample"),
                    make_option("--pl_name",              type = "character", help = "pipeline name",                                 default = "nf_starr_seq"),
                    make_option("--pl_version",           type = "character", help = "pipeline version",                              default = "dev"))

opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

# ==============================================================================
# Load modules
# ==============================================================================
source(file.path(opt$rscript_dir, "report_utils.R"))
source(file.path(opt$rscript_dir, "report_plots.R"))
source(file.path(opt$rscript_dir, "report_html.R"))

# ==============================================================================
# Prepare input file lists
# ==============================================================================
file_basic_stats <- opt$basic_stats
plots_basic_stats <- unlist(strsplit(opt$basic_plots, ","))

# ==============================================================================
# Prepare output directory
# ==============================================================================
if(!dir.exists(opt$output_dir)) dir.create(opt$output_dir, recursive = TRUE)
setwd(opt$output_dir)

report_prefix <- opt$prefix

# ==============================================================================
# Read input files
# ==============================================================================
message(format(Sys.time(), "[%Y-%m-%d %H:%M:%S] "), "Reading input files ...")


# ==============================================================================
# Generate report
# ==============================================================================
message(format(Sys.time(), "[%Y-%m-%d %H:%M:%S] "), "Creating final html report...")

file_render_context <- paste0(report_prefix, ".starr_seq_report.Rmd")
create_html_render(opt$pl_name,
                   opt$pl_version,
                   opt$lib_type,
                   file_basic_stats,
                   plots_basic_stats,
                   file_render_context)

rmarkdown::render(file_render_context, clean = TRUE, quiet = TRUE)
# invisible(file.remove(file_render_context))