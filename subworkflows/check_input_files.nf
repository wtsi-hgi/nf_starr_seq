import java.util.zip.GZIPInputStream
import java.io.InputStreamReader
import java.io.BufferedReader

workflow check_input_files {
    take:
    ch_input

    main:
    // Cross-row validation: collect all rows, check inter-row constraints, re-emit
    ch_validated = ch_input
        .collect()
        .map { rows ->
            def lib_sample_keys = rows.collect { [it[0], it[2]] }.unique()
            lib_sample_keys.each { lib, sample ->
                def group_rows  = rows.findAll { it[0] == lib && it[2] == sample }
                def has_output  = group_rows.any { it[1] == "output" }
                def has_input   = group_rows.any { it[1] == "input" }
                if (lib == "enhancer") {
                    if (has_output && !has_input) {
                        throw new IllegalArgumentException("Error: library '${lib}' sample '${sample}' has output but no input in the sample sheet.")
                    }
                } else if (lib in ["promoter", "random"]) {
                    if (has_output) {
                        def output_no_barcode = group_rows.any { it[1] == "output" && (!it[8] || it[8].trim() == '') }
                        if (output_no_barcode && !has_input) {
                            throw new IllegalArgumentException("Error: library '${lib}' sample '${sample}' has output with no barcode but no input in the sample sheet.")
                        }
                    }
                }
            }
            return rows
        }
        .flatMap { it }

    CHECK_FILES(ch_validated)
    ch_fastq   = CHECK_FILES.out.ch_fastq
    ch_ref     = CHECK_FILES.out.ch_ref
    ch_barcode = CHECK_FILES.out.ch_barcode

    emit:
    ch_fastq
    ch_ref
    ch_barcode
}

process CHECK_FILES {
    label 'process_single'

    tag "${library}_${type}_${sample}_${replicate}"

    input:
    tuple val(library), val(type), val(sample), val(replicate), val(directory), val(read1), val(read2), val(reference), val(barcode)

    output:
    tuple val(library), val(type), val(sample), val(replicate), path("${prefix}.r1.fastq.gz"), path("${prefix}.r2.fastq.gz"), emit: ch_fastq
    tuple val(library), val(type), val(sample), val(replicate), path("${prefix}.ref.fasta"),   emit: ch_ref,     optional: true
    tuple val(library), val(type), val(sample), val(replicate), path("${prefix}.barcode.tsv"), emit: ch_barcode, optional: true

    script:
    def file_read1     = file("${directory}/${read1}")
    def file_read2     = file("${directory}/${read2}")
    def file_reference = (reference && reference.trim() != '') ? file("${directory}/${reference}") : null
    def file_barcode   = (barcode   && barcode.trim()   != '') ? file("${directory}/${barcode}")   : null

    def valid_read_ext = [".fq", ".fastq", ".fq.gz", ".fastq.gz"]
    def valid_ref_ext  = [".fa", ".fasta"]
    def valid_bar_ext  = [".tsv"]

    def valid_libraries = ["enhancer", "promoter", "random"]
    if (!valid_libraries.contains(library)) {
        error("Error: library '${library}' is invalid. Expected one of: ${valid_libraries.join(', ')}")
    }

    def valid_types = ["input", "output", "template"]
    if (!valid_types.contains(type)) {
        error("Error: type '${type}' is invalid. Expected one of: ${valid_types.join(', ')}")
    }

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

    if (library == "enhancer") {
        if (file_reference == null) {
            error("Error: reference is required for library '${library}' but is empty.")
        }
        if (!file_reference.exists()) {
            error("Error: ${reference} is not found in ${directory}.")
        }
        if (!valid_ref_ext.any { reference.endsWith(it) }) {
            error("Error: File format for ${reference} is incorrect. Expected one of: ${valid_ref_ext.join(', ')}")
        }
    } else if (file_reference != null) {
        if (!file_reference.exists()) {
            error("Error: ${reference} is not found in ${directory}.")
        }
        if (!valid_ref_ext.any { reference.endsWith(it) }) {
            error("Error: File format for ${reference} is incorrect. Expected one of: ${valid_ref_ext.join(', ')}")
        }
    }

    if (file_barcode != null) {
        if (!file_barcode.exists()) {
            error("Error: ${barcode} is not found in ${directory}.")
        }
        if (!valid_bar_ext.any { barcode.endsWith(it) }) {
            error("Error: File format for ${barcode} is incorrect. Expected one of: ${valid_bar_ext.join(', ')}")
        }
    }

    def prefix = "${library}_${type}_${sample}_${replicate}"

    def resource_ref = file("${projectDir}/assets/resources/${reference}")
    def link_reference = null

    if (file_reference != null) {
        if (resource_ref.exists()) {
            log.info "Using reference from resources: ${resource_ref}"
            link_reference = resource_ref
        } else {
            log.info "Using reference from sample sheet: ${file_reference}"
            link_reference = file_reference
        }
    }

    """
    echo "Checking: ${prefix}"

    if [[ "${file_read1}" == *.fq || "${file_read1}" == *.fastq ]]; then
        gzip -c ${file_read1} > ${prefix}.r1.fastq.gz
    else
        ln -s ${file_read1} ${prefix}.r1.fastq.gz
    fi

    if [[ "${file_read2}" == *.fq || "${file_read2}" == *.fastq ]]; then
        gzip -c ${file_read2} > ${prefix}.r2.fastq.gz
    else
        ln -s ${file_read2} ${prefix}.r2.fastq.gz
    fi

    ${link_reference != null ? "ln -s ${link_reference} ${prefix}.ref.fasta" : ""}

    ${file_barcode != null ? "ln -s ${file_barcode} ${prefix}.barcode.tsv" : ""}
    """
}
