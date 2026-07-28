/* ---- splicing analysis pipeline ---- */

/* -- load modules -- */
include { NOTE_CMD }                  from "$projectDir/modules/local/init_workflow/main"
include { FASTP }                     from "$projectDir/modules/local/fastp/main"
include { FLASH2 }                    from "$projectDir/modules/local/flash2/main"

/* -- load subworkflows -- */
include { check_input_files }         from "$projectDir/subworkflows/check_input_files.nf"
include { preprocess }                from "$projectDir/subworkflows/preprocess.nf"
include { process_enhancer_lib }      from "$projectDir/subworkflows/process_enhancer_lib.nf"

/* -- define functions -- */
def helpMessage() {
    log.info """
Usage:
    nextflow run nf_starr_seq/main.nf --sample_sheet "/path/of/sample/sheet"

    Mandatory arguments:
        --sample_sheet                path of the sample sheet
        --outdir                      the directory path of output results, default: the current directory
        --resource                    the directory path of resource files, default: null

    Optional arguments:
    Cutadapt:
        --ct_a                        Sequence of an adapter ligated to the 3' end (paired data: of the first read)
        --ct_g                        Sequence of an adapter ligated to the 5' end (paired data: of the first read)
        --ct_A                        Sequence of an adapter ligated to the 3' end (paired data: of the second read)
        --ct_G                        Sequence of an adapter ligated to the 5' end (paired data: of the second read)
        --ct_O                        Require MINLENGTH overlap between read and adapter for an adapter to be found, default: 3
        --ct_e                        Maximum allowed error rate, default: 0.1 (10%)
        --ct_m                        Discard reads shorter than LEN, default: 0
        --ct_action                   What to do if a match was found, default: "trim" {trim, retain, mask, lowercase, none}

    Deduplication:
        --skip_dedup                  whether to skip deduplication, default: false
        --has_umi                     whether the reads contain UMIs, default: false
    
    Fastp (only for deduplication):
        --fp_q                        the quality value that a base is qualified, default: 20
        --fp_u                        how much percentage of bases are allowed to be unqualified (0~100), default: 30
        --fp_dup_calc_accuracy        the accuracy level for duplicate detection, default: 6
        --fp_umi_loc                  the location of UMI ["index1", "index2", "read1", "read2", "per_index", "per_read"], default: none
        --fp_umi_len                  the length of UMI when --fp_umi_loc in ["read1", "read2", "per_read"], default: 10
        --fp_umi_prefix               if specified, an underline will be used to connect prefix and UMI (i.e. prefix=UMI, UMI=AATTCG, final=UMI_AATTCG), default: none
        --fp_umi_skip                 if the UMI is in read1/read2, fastp can skip several bases following UMI, default: 0
        --fp_umi_delim                delimiter to use between the read name and the UMI, default: ":"

    Flash2:
        --skip_flash2                 whether to skip flash2, default: false
        --f2_min_overlap              min overlap for flash2, default: 10
        --f2_max_overlap              max overlap for flash2, default: 150
        --f2_min_overlap_outie        min overlap outie for flash2, default: 20
        --f2_max_mismatch_density     max mismatch density for flash2, default: 0.25

    Aligner:
        --aligner                     the aligner to use, default: bowtie2, options: [bwa, bowtie2]

    Bowtie2:
        --bt2_cut_len                 the length of cutting reads into segments for Bowtie2, default: 50

    BWA:
        --bwa_mismatch                mismatch penalty for BWA, default: 4
        --bwa_gap_open                gap open penalty for BWA, default: 10,10
        --bwa_gap_ext                 gap extension penalty for BWA, default: 5,5
        --bwa_clip                    clip penalty for BWA, default: 1,1

    MACS3:
        --macs3_g                     effective genome size, [hs, mm, ce, dm], default: hs
        --macs3_q                     minimum FDR (q-value) cutoff for peak detection, default: 0.05
        --macs3_extsize               when nomodel is true, MACS will use this value as fragment size

    StarrPeaker:
        --sp_threshold                starrpeaker threshold, default: 0.05
        --sp_length                   bin length, default: 500
        --sp_step                     step size, default: 100
        --sp_min                      minimum template size, default: 0
        --sp_max                      maximum template size, default: 1000
        --sp_mincov                   minimum Coverage, default: 0
    """
}

def check_software_exists(tool) {
    try {
        def process = ["which", tool].execute()
        process.waitFor()
        return process.exitValue() == 0
    } catch (Exception e) {
        return false
    }
}

def check_required(required_tools) {
    def missing_tools = required_tools.findAll { !check_software_exists(it) }

    log.info "====================================="
    log.info "Checking software:"
    required_tools.each { tool ->
        if (check_software_exists(tool)) {
            log.info "    |----> ${tool} is available"
        } else {
            log.info "    |----> ${tool} is not found"
        }
    }

    if (missing_tools) {
        error "Error: the following tools are missing: ${missing_tools.join(', ')}"
    }

    log.info "Done: all required tools are available. Proceeding with the pipeline."
    log.info "====================================="
}

/* -- pipeline info -- */
log.info """
=====================================
${workflow.manifest.name}
Version: ${workflow.manifest.version}
=====================================
"""

/* -- check parameters -- */
if (params.help) {
    helpMessage()
    exit 0
}

if (params.version) {
    println "${workflow.manifest.version}"
    exit 0
}

if (params.sample_sheet) {
    // reading sample sheet
    def sep = params.sample_sheet.endsWith('.tsv') ? '\t' : ','
    ch_input = Channel.fromPath(file(params.sample_sheet), checkIfExists: true)
                      .splitCsv(header: true, sep: sep)
    
    // check required columns
    def required_cols = ['library', 'type', 'sample', 'replicate', 'directory', 'read1', 'read2', 'reference', 'barcode', 'blacklist']
    def header_line = new File(params.sample_sheet).readLines().head()
    def header = header_line.split(sep)
    def missing = required_cols.findAll { !(it in header) }

    if (missing) {
        error "Error: Sample sheet is missing required columns - ${missing.join(', ')}"
    } else {
        def sheet_file = file(params.sample_sheet)
        log.info """
=====================================
sample sheet content:
-------------------------------------
${sheet_file.text}
=====================================
        """
    }
} else {
    error("Error: Please specify the full path of the sample sheet!\n")
}

def outdir = file(params.outdir)
if (!outdir.exists()) {
    log.info "Output directory does not exist, creating: ${outdir}"
    outdir.mkdirs()
}

if (!file(params.outdir).isDirectory()) {
    error("Invalid output directory: ${params.outdir}. Please specify a valid directory.")
}

if (!file(params.resource).isDirectory()) {
    error("Invalid resource directory: ${params.resource}. Please specify a valid directory.")
}

if (params.has_umi) {
    log.info "UMI information is provided. Deduplication will consider UMIs."
    
    if (params.fp_umi_loc == null) {
        error("Error: --fp_umi_loc must be specified when --has_umi is true.")
    } else if (params.fp_umi_loc in ["index1", "index2", "read1", "read2", "per_index", "per_read"]) {
        log.info "UMI location is set to '${params.fp_umi_loc}'."
    } else {
        error("Error: Invalid value for --fp_umi_loc: ${params.fp_umi_loc}. Expected one of: index1, index2, read1, read2, per_index, per_read.")
    }

    if (params.fp_umi_loc in ["read1", "read2", "per_read"] && (!params.fp_umi_len || params.fp_umi_len <= 0)) {
        error("Error: --fp_umi_len must be a positive integer when --fp_umi_loc is set to 'read1', 'read2', or 'per_read'.")
    }

    if (params.fp_umi_prefix) {
        log.info "UMI prefix is set to '${params.fp_umi_prefix}'. UMIs will be prefixed accordingly in the output."
    }

    if (params.fp_umi_skip && params.fp_umi_skip > 0) {
        log.info "UMI skip is set to ${params.fp_umi_skip}. Fastp will skip ${params.fp_umi_skip} bases following the UMI in the read."
    }
} else {
    log.info "No UMI information provided. Deduplication will be based on sequence alone."
}

/* -- check software exist -- */
def required_tools = ['fastqc', 'cutadapt', 'fastp', 'flash2', 'bwa', 'samtools', 'bowtie2', 'macs3', 'starrpeaker']
check_required(required_tools)

/* -- workflow -- */
workflow starr_seq {
    /* -- note down the command line -- */
    NOTE_CMD(workflow.commandLine)

    /* -- check input files exist -- */
    check_input_files(ch_input)
    ch_fastq     = check_input_files.out.ch_fastq
    ch_ref       = check_input_files.out.ch_ref
    ch_barcode   = check_input_files.out.ch_barcode
    ch_blacklist = check_input_files.out.ch_blacklist

    /* -- preprocess -- */
    preprocess(ch_fastq)
    ch_preprocessed_fastq = preprocess.out.ch_preprocessed_fastq

    /* -- split by library -- */
    ch_preprocessed_fastq
        .branch {
            enhancer: it[0] == "enhancer"
            promoter: it[0] == "promoter"
            random:   it[0] == "random"
        }
        .set { ch_preprocessed_by_lib }

    ch_enhancer = ch_preprocessed_by_lib.enhancer
    ch_promoter = ch_preprocessed_by_lib.promoter
    ch_random   = ch_preprocessed_by_lib.random

    /* -- process enhancer library -- */
    ch_enhancer = ch_enhancer.join(ch_ref, by: [0,1,2,3])
                             .join(ch_blacklist, by: [0,1,2,3], remainder: true)
    process_enhancer_lib(ch_enhancer)

    /* -- process promoter library -- */


    /* -- process random library -- */


}
