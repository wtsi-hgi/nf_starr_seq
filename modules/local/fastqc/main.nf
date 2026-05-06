process FASTQC {
    label 'process_low'

    publishDir "${params.outdir}/res_fastqc", mode: 'copy'

    tag "${library}_${sample}_${replicate}"

    input:
    tuple val(library), val(sample), val(replicate), path(read1), path(read2)

    output:
    tuple val(library), val(sample), val(replicate), path("${prefix}"), emit: ch_fastqc_html

    script:
    def prefix = "${library}_${sample}_${replicate}"

    """
    fastqc --threads ${task.cpus} --outdir ${prefix} ${read1} ${read2}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        fastqc: \$( fastqc --version | awk '{print \$2}')
    END_VERSIONS  
    """
}