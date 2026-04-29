process FASTQC {
    label 'process_low'

    publishDir "${params.outdir}/res_fastqc", mode: 'copy'

    tag "${sample}_${replicate}"

    input:
    tuple val(library), val(sample), val(replicate), path(read1), path(read2)

    output:
    tuple val(library), val(sample), val(replicate), path("${library}_${sample}_${replicate}"), emit: ch_fastqc_html

    script:
    """
    fastqc --threads ${task.cpus} --outdir ${library}_${sample}_${replicate} ${read1} ${read2}
    """
}