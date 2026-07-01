process BWA_SE {
    label 'process_medium'

    tag "${library}_${type}_${sample}_${replicate}"

    publishDir(
        path: "${params.outdir}/bwa_stats",
        mode: "copy",
        pattern: "*.flagstat.txt",
        overwrite: true
    )

    input:
    tuple val(library), val(type), val(sample), val(replicate), val(reference), path(read)

    output:
    tuple val(library), val(type), val(sample), val(replicate), 
          path("${library}_${type}_${sample}_${replicate}.unique.sort.bam"), 
          path("${library}_${type}_${sample}_${replicate}.unique.sort.bam.bai"), emit: ch_bam
    tuple val(library), val(type), val(sample), val(replicate), 
          path("${library}_${type}_${sample}_${replicate}.flagstat.txt"), 
          path("${library}_${type}_${sample}_${replicate}.unique.flagstat.txt"), emit: ch_flagstat

    script:
    def prefix = "${library}_${type}_${sample}_${replicate}"
    def bwa_index = "${params.resource}/bwa_index/${reference}"

    """
    bwa mem -t ${task.cpus} \
            -B ${params.bwa_mismatch} \
            -O ${params.bwa_gap_open} \
            -E ${params.bwa_gap_ext} \
            -L ${params.bwa_clip} \
            ${bwa_index} ${read} | samtools view -@ ${task.cpus} -bS - > ${prefix}.bam
    samtools flagstat ${prefix}.bam > ${prefix}.flagstat.txt

    samtools view -@ ${task.cpus} -b -F 256 -F 2048 ${prefix}.bam > ${prefix}.unique.bam
    samtools sort -@ ${task.cpus} -o ${prefix}.unique.sort.bam ${prefix}.unique.bam
    samtools index ${prefix}.unique.sort.bam
    samtools flagstat ${prefix}.unique.sort.bam > ${prefix}.unique.flagstat.txt
    rm ${prefix}.unique.bam

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        bwa: \$( bwa 2>&1 | grep -i version | awk '{print \$2}' )
        samtools: \$( samtools --version | head -n 1 | awk '{print \$2}' )
    END_VERSIONS
    """
}

process BWA_PE {
    label 'process_medium'

    tag "${library}_${type}_${sample}_${replicate}"

    publishDir(
        path: "${params.outdir}/bwa_stats",
        mode: "copy",
        pattern: "*.flagstat.txt",
        overwrite: true
    )

    input:
    tuple val(library), val(type), val(sample), val(replicate), val(reference), path(read1), path(read2)

    output:
    tuple val(library), val(type), val(sample), val(replicate), 
          path("${library}_${type}_${sample}_${replicate}.unique.sort.bam"), 
          path("${library}_${type}_${sample}_${replicate}.unique.sort.bam.bai"), emit: ch_bam
    tuple val(library), val(type), val(sample), val(replicate), 
          path("${library}_${type}_${sample}_${replicate}.flagstat.txt"), 
          path("${library}_${type}_${sample}_${replicate}.unique.flagstat.txt"), emit: ch_flagstat

    script:
    def prefix = "${library}_${type}_${sample}_${replicate}"
    def bwa_index = "${params.resource}/bwa_index/${reference}"
    
    """
    bwa mem -t ${task.cpus} \
            -B ${params.bwa_mismatch} \
            -O ${params.bwa_gap_open} \
            -E ${params.bwa_gap_ext} \
            -L ${params.bwa_clip} \
            ${bwa_index} ${read1} ${read2} | samtools view -@ ${task.cpus} -bS - > ${prefix}.bam
    samtools flagstat ${prefix}.bam > ${prefix}.flagstat.txt

    samtools view -@ ${task.cpus} -b -F 256 -F 2048 ${prefix}.bam > ${prefix}.unique.bam
    samtools sort -@ ${task.cpus} -o ${prefix}.unique.sort.bam ${prefix}.unique.bam
    samtools index ${prefix}.unique.sort.bam
    samtools flagstat ${prefix}.unique.sort.bam > ${prefix}.unique.flagstat.txt
    rm ${prefix}.unique.bam

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        bwa: \$( bwa 2>&1 | grep -i version | awk '{print \$2}' )
        samtools: \$( samtools --version | head -n 1 | awk '{print \$2}' )
    END_VERSIONS
    """
}
