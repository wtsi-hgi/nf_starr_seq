workflow generate_enhancer_report {
    take:
    ch_input

    main:
    GENERATE_ENHANCER_REPORT(ch_input)
}

process GENERATE_ENHANCER_REPORT {
    label 'process_single_dynamic_memory'
    
    memory {
        def file_size_total = basic_stats.size()
        def mem = file_size_total <= 100_000_000 ? 4 :
                  file_size_total <= 1_000_000_000 ? 8 :
                  file_size_total <= 2_000_000_000 ? 16 :
                  file_size_total <= 4_000_000_000 ? 32 : 64
        "${mem * task.attempt} GB"
    }
    
    publishDir(
        path: "${params.outdir}/final_reports",
        mode: "copy",
        overwrite: true
    )

    input:
    tuple val(library), val(file_basic_stats), val(plot_basic_num), val(plot_basic_pct)  

    output:
    tuple val(library), path("${library}.starr_seq_report.html"), emit: ch_html_report

    script:
    """
    ln -s ${projectDir}/assets/src/jquery-3.6.0.min.js jquery-3.6.0.min.js
    ln -s ${projectDir}/assets/src/select2.min.js select2.min.js
    ln -s ${projectDir}/assets/src/select2.min.css select2.min.css
        
    ${projectDir}/scripts/generate_enhancer_report.R --rscript_dir ${projectDir}/scripts \
                                                     --lib_type    ${library} \
                                                     --basic_stats ${file_basic_stats} \
                                                     --basic_plots ${plot_basic_num},${plot_basic_pct} \
                                                     --prefix      ${library} \
                                                     --pl_name     ${params.pipeline_name} \
                                                     --pl_version  ${params.pipeline_version}                                                     

    """
}
