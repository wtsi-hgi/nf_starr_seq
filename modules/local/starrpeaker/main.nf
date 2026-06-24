process STARRPEAKER_CALLPEAKS {
    label 'process_high_dynamic_memory'

    memory {
        def file_size = input_bam.size()
        def mem = file_size <= 4_000_000_000 ? 8 :
                  file_size <= 8_000_000_000 ? 16 :
                  file_size <= 16_000_000_000 ? 32 :
                  file_size <= 32_000_000_000 ? 64 : 128
        "${mem * task.attempt} GB"
    }

    tag "${library}_${sample}_${replicate}"

    input:
    tuple val(library), val(sample), val(replicate), path(output_bam), path(output_bai), path(input_bam), path(input_bai), path(ref)

    output:
    tuple val(library), val(sample), val(replicate), 
          path("${prefix}.input.bw"),
          path("${prefix}.output.bw"),
          path("${prefix}.peak.bed"), 
          path("${prefix}.peak.final.bed"), emit: ch_starrpeaker_peaks

    script:
    def prefix = "${library}_${sample}_${replicate}"
    def do_se = params.skip_flash2 ? "" : "--se"

    def cov_files = ""


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
