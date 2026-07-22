process BAMCOVERAGE {
    label 'process_medium'

    tag "${library}_${type}_${sample}_${replicate}"

    publishDir "${params.outdir}/enhancer_bigwig/${library}_${type}_${sample}_${replicate}", mode: "copy", overwrite: true

    input:
    tuple val(library), val(type), val(sample), val(replicate), path(bam), path(bai)

    output:
    tuple val(library), val(type), val(sample), val(replicate), 
          path("${library}_${type}_${sample}_${replicate}.cpm.bw"), emit: ch_bam_bigwig    
    
    script:
    def prefix = "${library}_${type}_${sample}_${replicate}"
    def extend_args = params.skip_flash2 ? "--extendReads" : ""

    """
    bamCoverage --bam ${bam} \
                --outFileName ${prefix}.cpm.bw \
                --outFileFormat bigwig \
                --binSize ${params.bigwig_bin} \
                --normalizeUsing CPM \
                ${extend_args} \
                --numberOfProcessors ${task.cpus}
    """
}
