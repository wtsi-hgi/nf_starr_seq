process STARRPEAKER_CALLPEAKS {
    label 'process_high_dynamic_memory'

    memory {
        def file_size = input_bam.size()
        def mem = file_size <= 4_000_000_000 ? 16 :
                  file_size <= 8_000_000_000 ? 32 :
                  file_size <= 16_000_000_000 ? 64 :
                  file_size <= 32_000_000_000 ? 128 : 256
        "${mem * task.attempt} GB"
    }

    tag "${library}_${sample}_${replicate}"

    publishDir "${params.outdir}/enhancer_peaks/${library}_${sample}_${replicate}/starrpeaker", mode: "copy", overwrite: true

    input:
    tuple val(library), val(sample), val(replicate), path(output_bam), path(output_bai), path(input_bam), path(input_bai), val(reference)

    output:
    tuple val(library), val(sample), val(replicate), 
          path("${library}_${sample}_${replicate}.input.bw"),
          path("${library}_${sample}_${replicate}.output.bw"),
          path("${library}_${sample}_${replicate}.peak.bed"), 
          path("${library}_${sample}_${replicate}.peak.final.bed"), emit: ch_starrpeaker_peaks

    script:
    def prefix = "${library}_${sample}_${replicate}"
    def do_se = params.skip_flash2 ? "" : "--se"

    def chromsize = "${params.resource}/starrpeaker/${reference}.chromsize.tsv"
    def blacklist = "${params.resource}/starrpeaker/${reference}.blacklist.bed"
    def gc_file   = "${params.resource}/starrpeaker/${reference}.ucsc-gc-5bp.bw"
    def map_file  = "${params.resource}/starrpeaker/${reference}.gem-mappability-100mer.bw"
    def fold_file = "${params.resource}/starrpeaker/${reference}.linearfold-folding-energy-100bp.bw"

    """
    starrpeaker ${do_se} \
                --prefix ${prefix} \
                --chromsize ${chromsize} \
                --blacklist ${blacklist} \
                --input ${input_bam} \
                --output ${output_bam} \
                --threshold ${params.sp_threshold} \
                --cov ${gc_file} ${map_file} ${fold_file} \
                --length ${params.sp_length} \
                --step ${params.sp_step} \
                --min ${params.sp_min} \
                --max ${params.sp_max} \
                --mincov ${params.sp_mincov}
    """
}
