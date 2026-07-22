process BOWTIE2_SE {
    label 'process_medium'

    tag "${library}_${type}_${sample}_${replicate}"

    publishDir(
        path: "${params.outdir}/enhancer_bowtie2_stats",
        mode: "copy",
        pattern: "*.flagstat.txt",
        overwrite: true
    )

    input:
    tuple val(library), val(type), val(sample), val(replicate), val(reference), path(read)

    output:
    tuple val(library), val(type), val(sample), val(replicate), 
          path("${library}_${type}_${sample}_${replicate}.unique.bam"), 
          path("${library}_${type}_${sample}_${replicate}.unique.bam.bai"), emit: ch_bam
    tuple val(library), val(type), val(sample), val(replicate), 
          path("${library}_${type}_${sample}_${replicate}.flagstat.txt"), 
          path("${library}_${type}_${sample}_${replicate}.unique.flagstat.txt"), emit: ch_flagstat

    script:
    def prefix = "${library}_${type}_${sample}_${replicate}"
    def bowtie2_index = "${params.resource}/bowtie2_index/${reference}"

    """
    cutadapt -l ${params.bt2_cut_len} \
             -o ${prefix}.bt2_cut.fastq.gz \
             -j ${task.cpus} \
             ${read} > ${prefix}.bt2_cut.log

    bowtie2 -x ${bowtie2_index} \
            -U ${prefix}.bt2_cut.fastq.gz \
            --maxins ${params.bt2_maxins} \
            -p ${task.cpus} \
            --rg-id ${prefix} \
            --rg "SM:${prefix}" \
            --rg "LB:${prefix}" \
            --rg "PL:ILLUMINA" \
            --rg "PU:unit1" \
            2> ${prefix}.bowtie2.log \
            | samtools sort -@ ${task.cpus} -o ${prefix}.bam -
    samtools index ${prefix}.bam
    samtools flagstat ${prefix}.bam > ${prefix}.flagstat.txt

    samtools view -@ ${task.cpus} -b -F 4 -F 256 -F 2048 ${prefix}.bam -q ${params.aligner_min_mapq} > ${prefix}.unique.bam
    samtools index ${prefix}.unique.bam
    samtools flagstat ${prefix}.unique.bam > ${prefix}.unique.flagstat.txt

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        cutadapt: \$( cutadapt --version )
        bowtie2: \$( bowtie2 --version | awk '{print \$3}' | head -n 1 )
        samtools: \$( samtools --version | head -n 1 | awk '{print \$2}' )
    END_VERSIONS
    """
}

process BOWTIE2_PE {
    label 'process_medium'

    tag "${library}_${type}_${sample}_${replicate}"

    publishDir(
        path: "${params.outdir}/enhancer_bowtie2_stats",
        mode: "copy",
        pattern: "*.flagstat.txt",
        overwrite: true
    )

    input:
    tuple val(library), val(type), val(sample), val(replicate), val(reference), path(read1), path(read2)

    output:
    tuple val(library), val(type), val(sample), val(replicate), 
          path("${library}_${type}_${sample}_${replicate}.unique.bam"), 
          path("${library}_${type}_${sample}_${replicate}.unique.bam.bai"), emit: ch_bam
    tuple val(library), val(type), val(sample), val(replicate), 
          path("${library}_${type}_${sample}_${replicate}.flagstat.txt"), 
          path("${library}_${type}_${sample}_${replicate}.unique.flagstat.txt"), emit: ch_flagstat

    script:
    def prefix = "${library}_${type}_${sample}_${replicate}"
    def bowtie2_index = "${params.resource}/bowtie2_index/${reference}"
    
    """
    cutadapt -l ${params.bt2_cut_len} \
             -o ${prefix}.bt2_cut.r1.fastq.gz \
             -p ${prefix}.bt2_cut.r2.fastq.gz \
             -j ${task.cpus} \
             ${read1} ${read2} > ${prefix}.bt2_cut.log

    bowtie2 -x ${bowtie2_index} \
            -1 ${prefix}.bt2_cut.r1.fastq.gz \
            -2 ${prefix}.bt2_cut.r2.fastq.gz \
            --maxins ${params.bt2_maxins} \
            -p ${task.cpus} \
            --rg-id ${prefix} \
            --rg "SM:${prefix}" \
            --rg "LB:${prefix}" \
            --rg "PL:ILLUMINA" \
            --rg "PU:unit1" \
            2> ${prefix}.bowtie2.log \
            | samtools sort -@ ${task.cpus} -o ${prefix}.bam -
    samtools index ${prefix}.bam
    samtools flagstat ${prefix}.bam > ${prefix}.flagstat.txt

    samtools view -@ ${task.cpus} -b -f 2 -F 256 -F 2048 ${prefix}.bam -q ${params.aligner_min_mapq} > ${prefix}.unique.bam
    samtools index ${prefix}.unique.bam
    samtools flagstat ${prefix}.unique.bam > ${prefix}.unique.flagstat.txt

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        cutadapt: \$( cutadapt --version )
        bowtie2: \$( bowtie2 --version | awk '{print \$3}' | head -n 1 )
        samtools: \$( samtools --version | head -n 1 | awk '{print \$2}' )
    END_VERSIONS
    """
}
