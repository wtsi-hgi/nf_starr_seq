import java.util.zip.GZIPInputStream
import java.io.InputStreamReader
import java.io.BufferedReader

workflow check_input_files {
    take:
    ch_sample

    main:
    CHECK_FILES(ch_sample)
    ch_fastq = CHECK_FILES.out.ch_fastq

    emit:
    ch_fastq
}

process CHECK_FILES {
    label 'process_single'

    tag "${library}_${sample}_${replicate}"

    input:
    tuple val(library), val(sample), val(replicate), val(directory), val(read1), val(read2), val(reference)

    output:
    tuple val(library), val(sample), val(replicate), path("${library}_${sample}_${replicate}.r1.fastq.gz"), path("${library}_${sample}_${replicate}.r2.fastq.gz"), emit: ch_fastq
    tuple val(library), val(sample), val(replicate), path("${library}.ref.fasta"), emit: ch_ref

    script:
    def file_read1 = file("${directory}/${read1}")
    def file_read2 = file("${directory}/${read2}")
    def file_reference = file("${directory}/${reference}")
    
    def valid_read_ext = [".fq", ".fastq", ".fq.gz", ".fastq.gz"]
    def valid_ref_ext = [".fa", ".fasta"]

    if (file_read1.exists()) {
        if (!valid_read_ext.any { read1.endsWith(it) }) {
            error("Error: File format for ${read1} is incorrect. Expected one of: ${valid_read_ext.join(', ')}")
        }
    } else {
        error("Error: ${read1} is not found in ${directory}.")
    }

    if (file_read2.exists()) {
        if (!valid_read_ext.any { read2.endsWith(it) }) {
            error("Error: File format for ${read2} is incorrect. Expected one of: ${valid_read_ext.join(', ')}")
        }
    } else {
        error("Error: ${read2} is not found in ${directory}.")
    }

    if (file_reference.exists()) {
        if (!valid_ref_ext.any { reference.endsWith(it) }) {
            error("Error: File format for ${reference} is incorrect. Expected one of: ${valid_ref_ext.join(', ')}")
        }
    } else {
        error("Error: ${reference} is not found in ${directory}.")
    }

    def valid_libraries = ["enhance", "promoter", "random", "ts_promoter", "ts_random"]
    if (!valid_libraries.contains(library)) {
        error("Error: library '${library}' is invalid. Expected one of: ${valid_libraries.join(', ')}")
    }

    """
    echo "Checking: ${sample}"

    if [[ "${file_read1}" == *.fq || "${file_read1}" == *.fastq ]]; then
        gzip -c ${file_read1} > ${library}_${sample}_${replicate}.r1.fastq.gz
    else
        ln -s ${file_read1} ${library}_${sample}_${replicate}.r1.fastq.gz
    fi

    if [[ "${file_read2}" == *.fq || "${file_read2}" == *.fastq ]]; then
        gzip -c ${file_read2} > ${library}_${sample}_${replicate}.r2.fastq.gz
    else
        ln -s ${file_read2} ${library}_${sample}_${replicate}.r2.fastq.gz
    fi

    ln -s ${file_reference} ${library}.ref.fasta
    """
}
