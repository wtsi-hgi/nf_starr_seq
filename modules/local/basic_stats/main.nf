process BASIC_STATS {
    label 'process_single'

    tag "${library}_${type}_${sample}_${replicate}"

    input:
    tuple val(library), val(type), val(sample), val(replicate), 
          path(fastp_stats), path(align_stats), path(align_unique_stats), path(picard_stats)

    output:
    tuple val(library), 
          path("${library}.final_stats.tsv"), 
          path("${library}.num_reads_barplot.png"), 
          path("${library}.pct_reads_barplot.png"), emit: ch_basic_stats_outs

    script:
    def list_sample_ids   = sample.join(',')
    def list_rep_ids      = replicate.join(',')
    def list_fastp_stats  = fastp_stats.join(',')
    def list_align_stats  = align_unique_stats.join(',')
    def list_picard_stats = picard_stats.join(',')

    """
    python ${projectDir}/scripts/parse_basic_stats.py --sample_ids    ${list_sample_ids} \
                                                      --rep_ids       ${list_rep_ids} \
                                                      --fastp_stats   ${list_fastp_stats} \
                                                      --align_stats   ${list_align_stats} \
                                                      --dedup_stats   ${list_picard_stats} \
                                                      --output_prefix ${library}
    """
}
