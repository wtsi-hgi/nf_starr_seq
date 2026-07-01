process FLASH2 {
    label 'process_medium_dynamic_memory'

    memory {
        def file_size_1 = read1.size()
        def file_size_2 = read2.size()
        def file_size_total = file_size_1 + file_size_2
        def mem = file_size_total <= 40_000_000_000 ? 20 :
                  file_size_total <= 80_000_000_000 ? 40 :
                  file_size_total <= 160_000_000_000 ? 80 :
                  file_size_total <= 320_000_000_000 ? 160 : 320
        "${mem * task.attempt} GB"
    }

    tag "${library}_${type}_${sample}_${replicate}"

    publishDir(
        path: "${params.outdir}/flash2_stats",
        mode: "copy",
        pattern: "*.merge_stats.tsv",
        overwrite: true
    )

    input:
    tuple val(library), val(sample), val(replicate), path(read1), path(read2)
    
    output:
    tuple val(library), val(type), val(sample), val(replicate), 
          path("${library}_${type}_${sample}_${replicate}.extendedFrags.fastq.gz"), emit: ch_extended_frags
    tuple val(library), val(type), val(sample), val(replicate), 
          path("${library}_${type}_${sample}_${replicate}.notCombined_1.fastq.gz"), 
          path("${library}_${type}_${sample}_${replicate}.notCombined_2.fastq.gz"), emit: ch_not_combined
    tuple val(library), val(type), val(sample), val(replicate), 
          path("${library}_${type}_${sample}_${replicate}.merge_stats.tsv"), emit: ch_merge_stats

    script:
    def prefix = "${library}_${type}_${sample}_${replicate}"

    """
    flash2 --min-overlap          ${params.f2_min_overlap} \
           --max-overlap          ${params.f2_max_overlap} \
           --min-overlap-outie    ${params.f2_min_overlap_outie} \
           --max-mismatch-density ${params.f2_max_mismatch_density} \
           --threads              ${task.cpus} \
           --output-prefix        ${prefix} \
           --output-directory     . \
           --compress \
           ${read1} ${read2} 2>&1 | tee ${prefix}.merge_stats.tsv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        flash2: \$( flash2 --version | head -n 1 | awk '{print \$2}' )
    END_VERSIONS
    """
}