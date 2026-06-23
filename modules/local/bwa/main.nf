process BWA_SE {
    label 'process_medium'

    tag "${library}_${type}_${sample}_${replicate}"

    input:
    tuple val(library), val(type), val(sample), val(replicate), path(reference), path(read)

    output:
    tuple val(library), val(type), val(sample), val(replicate), path("${prefix}.bam"), emit: ch_se_bam

    script:
    def prefix = "${library}_${type}_${sample}_${replicate}"
    def ref_base = reference.baseName
    def bwa_index = [ "${projectDir}/assets/resources/${ref_base}.amb", 
                      "${projectDir}/assets/resources/${ref_base}.ann", 
                      "${projectDir}/assets/resources/${ref_base}.bwt", 
                      "${projectDir}/assets/resources/${ref_base}.pac", 
                      "${projectDir}/assets/resources/${ref_base}.sa"  ]

    def has_index = bwa_index.every { file(it).exists() }

    if( has_index ) {
        """
        bwa mem -t ${task.cpus} \
                -B ${params.bwa_mismatch} \
                -O ${params.bwa_gap_open} \
                -E ${params.bwa_gap_ext} \
                -L ${params.bwa_clip} \
                ${reference} ${read} | samtools view -@ ${task.cpus} -bS - > ${prefix}.bam

        samtools view -@ ${task.cpus} -b -F 256 -F 2048 ${prefix}.bam > ${prefix}.unique.bam
        samtools sort -@ ${task.cpus} -o ${prefix}.unique.sort.bam ${prefix}.unique.bam
        samtools index ${prefix}.unique.sort.bam
        rm ${prefix}.unique.bam

        cat <<-END_VERSIONS > versions.yml
        "${task.process}":
            bwa: \$( bwa 2>&1 | grep -i version | awk '{print \$2}' )
            samtools: \$( samtools --version | head -n 1 | awk '{print \$2}' )
        END_VERSIONS
        """
    } else {
        """
        bwa index ${reference}

        bwa mem -t ${task.cpus} \
                -B ${params.bwa_mismatch} \
                -O ${params.bwa_gap_open} \
                -E ${params.bwa_gap_ext} \
                -L ${params.bwa_clip} \
                ${reference} ${read} | samtools view -@ ${task.cpus} -bS - > ${prefix}.bam

        samtools view -@ ${task.cpus} -b -F 256 -F 2048 ${prefix}.bam > ${prefix}.unique.bam
        samtools sort -@ ${task.cpus} -o ${prefix}.unique.sort.bam ${prefix}.unique.bam
        samtools index ${prefix}.unique.sort.bam
        rm ${prefix}.unique.bam

        cat <<-END_VERSIONS > versions.yml
        "${task.process}":
            bwa: \$( bwa 2>&1 | grep -i version | awk '{print \$2}' )
            samtools: \$( samtools --version | head -n 1 | awk '{print \$2}' )
        END_VERSIONS
        """
    }
}

process BWA_PE {
    label 'process_medium'

    tag "${library}_${type}_${sample}_${replicate}"

    input:
    tuple val(library), val(type), val(sample), val(replicate), path(reference), path(read1), path(read2)

    output:
    tuple val(library), val(type), val(sample), val(replicate), path("${prefix}.bam"), emit: ch_pe_bam

    script:
    def prefix = "${library}_${type}_${sample}_${replicate}"
    def ref_base = reference.baseName
    def bwa_index = [ "${projectDir}/assets/resources/${ref_base}.amb", 
                      "${projectDir}/assets/resources/${ref_base}.ann", 
                      "${projectDir}/assets/resources/${ref_base}.bwt", 
                      "${projectDir}/assets/resources/${ref_base}.pac", 
                      "${projectDir}/assets/resources/${ref_base}.sa"  ]

    def has_index = bwa_index.every { file(it).exists() }

    if( has_index ) {
        """
        bwa mem -t ${task.cpus} \
                -B ${params.bwa_mismatch} \
                -O ${params.bwa_gap_open} \
                -E ${params.bwa_gap_ext} \
                -L ${params.bwa_clip} \
                ${reference} ${read1} ${read2} | samtools view -@ ${task.cpus} -bS - > ${prefix}.bam

        samtools view -@ ${task.cpus} -b -F 256 -F 2048 ${prefix}.bam > ${prefix}.unique.bam
        samtools sort -@ ${task.cpus} -o ${prefix}.unique.sort.bam ${prefix}.unique.bam
        samtools index ${prefix}.unique.sort.bam
        rm ${prefix}.unique.bam

        cat <<-END_VERSIONS > versions.yml
        "${task.process}":
            bwa: \$( bwa 2>&1 | grep -i version | awk '{print \$2}' )
            samtools: \$( samtools --version | head -n 1 | awk '{print \$2}' )
        END_VERSIONS
        """
    } else {
        """
        bwa index ${reference}

        bwa mem -t ${task.cpus} \
                -B ${params.bwa_mismatch} \
                -O ${params.bwa_gap_open} \
                -E ${params.bwa_gap_ext} \
                -L ${params.bwa_clip} \
                ${reference} ${read1} ${read2} | samtools view -@ ${task.cpus} -bS - > ${prefix}.bam

        samtools view -@ ${task.cpus} -b -F 256 -F 2048 ${prefix}.bam > ${prefix}.unique.bam
        samtools sort -@ ${task.cpus} -o ${prefix}.unique.sort.bam ${prefix}.unique.bam
        samtools index ${prefix}.unique.sort.bam
        rm ${prefix}.unique.bam

        cat <<-END_VERSIONS > versions.yml
        "${task.process}":
            bwa: \$( bwa 2>&1 | grep -i version | awk '{print \$2}' )
            samtools: \$( samtools --version | head -n 1 | awk '{print \$2}' )
        END_VERSIONS
        """
    }
}