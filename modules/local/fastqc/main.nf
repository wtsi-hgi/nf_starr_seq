process FASTQC {
    label 'process_low'

    publishDir "${params.outdir}/1_fastqc", mode: 'copy'

    tag "${sample}_${replicate}"

    input:
    tuple val(sample), val(replicate), path(read1), path(read2)

    output:
    tuple val(sample), val(replicate), path("*.html"), emit: ch_fastqc_html

    script:
    """
    fastqc --threads ${task.cpus} ${read1} ${read2}
    """
}