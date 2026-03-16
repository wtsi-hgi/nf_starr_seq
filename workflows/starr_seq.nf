/* ---- splicing analysis pipeline ---- */

/* -- load modules -- */


/* -- load subworkflows -- */


/* -- define functions -- */
def helpMessage() {
    log.info """
Usage:
    nextflow run nf_starr_seq/main.nf --sample_sheet "/path/of/sample/sheet"

    Mandatory arguments:
    """
}

/* -- initialising parameters -- */
params.help                        = false
params.version                     = false
params.pipeline_name               = workflow.manifest.name
params.pipeline_version            = workflow.manifest.version


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



/* -- check software exist -- */



/* -- workflow -- */
workflow starr_seq {
    
}
