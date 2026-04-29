process CUTADAPT {
    label 'process_low'

    publishDir "${params.outdir}/res_cutadapt", mode: 'copy'

    tag "${sample}_${replicate}"

    input:
    tuple val(library), val(sample), val(replicate), path(read1), path(read2)

    output:
    tuple val(library), val(sample), val(replicate), path("${sample}_${replicate}.cutadapt.r1.fastq.gz"), path("${sample}_${replicate}.cutadapt.r2.fastq.gz"), emit: ch_cutadapt_fastq

    script:
    """
    cutadapt -j ${task.cpus} \
             -a ${params.ct_a} \
             -g ${params.ct_g} \
             -A ${params.ct_A} \
             -G ${params.ct_G} \
             -O ${params.ct_O} \
             -e ${params.ct_e} \
             -m ${params.ct_m} \
             -action ${params.ct_action} \
             -o ${sample}_${replicate}.cutadapt.r1.fastq.gz \
             -p ${sample}_${replicate}.cutadapt.r2.fastq.gz \
             ${read1} ${read2}
    """
}