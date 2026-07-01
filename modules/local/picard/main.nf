process PICARD_DEDUP {
    label 'process_single_dynamic_memory'

    memory {
        def file_size = bam.size()
        def mem = file_size <= 4_000_000_000 ? 16 :
                  file_size <= 8_000_000_000 ? 32 :
                  file_size <= 16_000_000_000 ? 64 :
                  file_size <= 32_000_000_000 ? 128 : 256
        "${mem * task.attempt} GB"
    }

    tag "${library}_${type}_${sample}_${replicate}"

    publishDir(
        path: "${params.outdir}/picard_stats",
        mode: "copy",
        pattern: "*.picard_dedup.flagstat.txt",
        overwrite: true
    )

    input:
    tuple val(library), val(type), val(sample), val(replicate), path(bam), path(bai)

    output:
    tuple val(library), val(type), val(sample), val(replicate), 
          path("${library}_${type}_${sample}_${replicate}.picard_dedup.bam"), 
          path("${library}_${type}_${sample}_${replicate}.picard_dedup.bam.bai"), emit: ch_picard_bam
    tuple val(library), val(type), val(sample), val(replicate), 
          path("${library}_${type}_${sample}_${replicate}.picard_dedup.flagstat.txt"), emit: ch_picard_flagstat

    script:
    def prefix = "${library}_${type}_${sample}_${replicate}"

    """
    picard MarkDuplicates --INPUT ${bam} \
                          --METRICS_FILE ${prefix}.picard_metrics.txt \
                          --OUTPUT ${prefix}.picard_dedup.bam \
                          --REMOVE_DUPLICATES true

    samtools index ${prefix}.picard_dedup.bam
    samtools flagstat ${prefix}.picard_dedup.bam > ${prefix}.picard_dedup.flagstat.txt

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        picard: \$( picard MarkDuplicates --version )
    END_VERSIONS
    """
}
