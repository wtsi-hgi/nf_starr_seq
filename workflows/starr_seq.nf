/* ---- splicing analysis pipeline ---- */

/* -- load modules -- */


/* -- load subworkflows -- */


/* -- define functions -- */
def helpMessage() {
    log.info """
Usage:
    nextflow run nf_starr_seq/main.nf --sample_sheet "/path/of/sample/sheet"

    Mandatory arguments:
        --sample_sheet                path of the sample sheet
        --outdir                      the directory path of output results, default: the current directory
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

/* -- initialising parameters -- */
params.help                        = false
params.version                     = false
params.pipeline_name               = workflow.manifest.name
params.pipeline_version            = workflow.manifest.version

params.sample_sheet                = null
params.outdir                      = params.outdir                      ?: "$PWD"

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
    def required_cols = ['library', 'sample', 'replicate', 'directory', 'read1', 'read2']
    def header_line = new File(params.sample_sheet).readLines().head()
    def header = header_line.split(sep)
    def missing = required_cols.findAll { !(it in header) }

    if (missing) {
        error "Error: Sample sheet is missing required columns - ${missing.join(', ')}"
    } else {
        def sheet_file = file(params.sample_sheet)
        log.info("=====================================")
        log.info("Sample sheet content:")
        log.info("-------------------------------------")
        log.info(sheet_file.text)
        log.info("=====================================")

        // reformat channel
        ch_input = ch_input.map { row -> 
            def sample_id = "${row.sample}_${row.replicate}"
            tuple(sample_id, row.sample, row.replicate, row.directory, row.read1, row.read2) }
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



/* -- check software exist -- */
def required_tools = ['fastqc', 'cutadapt', 'bwa', 'samtools']
check_required(required_tools)


/* -- workflow -- */
workflow starr_seq {
    
}
