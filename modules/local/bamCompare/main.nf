process BAMCOMPARE {
    label 'process_medium'

    tag "${library}_${sample}_${replicate}"

    publishDir "${params.outdir}/enhancer_bigwig/${library}_${type}_${sample}_${replicate}", mode: "copy", overwrite: true

    input:
    tuple val(library), val(sample), val(replicate), path(output_bam), path(output_bai), path(input_bam), path(input_bai)

    output:
    tuple val(library), val(sample), val(replicate), 
          path("${library}_${sample}_${replicate}.log2ratio.bw"), emit: ch_bam_bigwig_log2

    script:
    def prefix = "${library}_${sample}_${replicate}"
    def extend_args = params.skip_flash2 ? "--extendReads" : ""

    """
    bamCompare --bamfile1 ${output_bam} \
               --bamfile2 ${input_bam} \
               --outFileName ${prefix}.log2ratio.bw \
               --outFileFormat bigwig \
               --binSize ${params.bigwig_bin} \
               --scaleFactorsMethod readCount \
               ${extend_args} \
               --numberOfProcessors ${task.cpus}
    """
}
