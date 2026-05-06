process FASTP {
    label 'process_low'

    tag "${library}_${sample}_${replicate}"

    input:
    tuple val(library), val(sample), val(replicate), path(read1), path(read2)

    output:
    tuple val(library), val(sample), val(replicate), path("${prefix}.dedup.r1.fastq.gz"), path("${prefix}.dedup.r2.fastq.gz"), emit: ch_dedup_fastq
    tuple val(library), val(sample), val(replicate), path("${prefix}.dedup_stat.tsv"), emit: ch_dedup_stat

    script:
    def prefix = "${library}_${sample}_${replicate}"

    
    
    """
    fastp --in1               ${read1} \
          --in2               ${read2} \
          --out1              ${prefix}.dedup.r1.fastq.gz \
          --out2              ${prefix}.dedup.r2.fastq.gz \
          --compression       9 \
          --cut_tail \
          --cut_mean_quality  ${params.fastp_cut_mean_quality} \
          --thread            ${task.cpus} \
          --html              ${prefix}.dedup_stat.html 2>&1 | tee ${prefix}.dedup_stat.tsv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        fastp: \$( fastp --version | awk '{print \$2}' )
    END_VERSIONS    
    """
}