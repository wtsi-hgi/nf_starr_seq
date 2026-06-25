process MACS3_CALLPEAKS {
    label 'process_single_dynamic_memory'

    memory {
        def file_size = input_bam.size()
        def mem = file_size <= 4_000_000_000 ? 4 :
                  file_size <= 8_000_000_000 ? 8 :
                  file_size <= 16_000_000_000 ? 16 :
                  file_size <= 32_000_000_000 ? 32 : 64
        "${mem * task.attempt} GB"
    }

    tag "${library}_${sample}_${replicate}"

    publishDir "${params.outdir}/enhancer_peaks/${library}_${sample}_${replicate}/macs3", mode: "copy", overwrite: true

    input:
    tuple val(library), val(sample), val(replicate), path(output_bam), path(output_bai), path(input_bam), path(input_bai)

    output:
    tuple val(library), val(sample), val(replicate), 
          path("${library}_${sample}_${replicate}_peaks.narrowPeak"), 
          path("${library}_${sample}_${replicate}_peaks.xls"), 
          path("${library}_${sample}_${replicate}_summits.bed"), emit: ch_macs3_peaks

    script:
    def prefix = "${library}_${sample}_${replicate}"

    """
    macs callpeak -t ${output_bam} \
                  -c ${input_bam} \
                  -g ${params.macs3_g} \
                  -q ${params.macs3_q} \
                  -n ${prefix} \
                  --nomodel \
                  --extsize ${params.macs3_extsize}
    """
}
