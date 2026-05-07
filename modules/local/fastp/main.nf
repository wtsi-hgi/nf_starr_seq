process FASTP {
    label 'process_low'

    publishDir "${params.outdir}/res_fastp", mode: 'copy'

    tag "${library}_${sample}_${replicate}"

    input:
    tuple val(library), val(sample), val(replicate), path(read1), path(read2)

    output:
    tuple val(library), val(sample), val(replicate), path("${prefix}.dedup.r1.fastq.gz"), path("${prefix}.dedup.r2.fastq.gz"), emit: ch_dedup_fastq
    tuple val(library), val(sample), val(replicate), path("${prefix}.dedup_stats.tsv"), emit: ch_dedup_stats

    script:
    def prefix = "${library}_${sample}_${replicate}"
    def fastp_umi_args = ""

    if (library != "enhancer") {
        if (params.has_umi) {
            fastp_umi_args = ["--umi"]

            fastp_umi_args << "--umi_loc ${params.fp_umi_loc}"

            if (params.fp_umi_loc in ["read1", "read2", "per_read"]) {
                fastp_umi_args << "--umi_len ${params.fp_umi_len}"
            }

            if (params.fp_umi_prefix) {
                fastp_umi_args << "--umi_prefix ${params.fp_umi_prefix}"
            }

            if (params.fp_umi_skip > 0) {
                fastp_umi_args << "--umi_skip ${params.fp_umi_skip}"
            }

            fastp_umi_args << "--umi_delim ${params.fp_umi_delim}"

            fastp_umi_args = fastp_umi_args.join(" ")
        }
    }

    """
    fastp --in1 ${read1} \
          --in2 ${read2} \
          --out1 ${prefix}.dedup.r1.fastq.gz \
          --out2 ${prefix}.dedup.r2.fastq.gz \
          --compression 9 \
          --disable_adapter_trimming \
          --dedup \
          --dup_calc_accuracy ${params.fp_dup_calc_accuracy} \
          ${fastp_umi_args} \
          --thread ${task.cpus} \
          --html ${prefix}.dedup_stats.html 2>&1 | tee ${prefix}.dedup_stats.tsv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        fastp: \$( fastp --version | awk '{print \$2}' )
    END_VERSIONS    
    """
}