include { FASTQC }   from "$projectDir/modules/local/fastqc/main"
include { CUTADAPT } from "$projectDir/modules/local/cutadapt/main"

workflow preprocess {
    take:
    ch_fastq

    main:
    FASTQC(ch_fastq)
    ch_fastqc_html = FASTQC.out.ch_fastqc_html

    if (params.ct_a || params.ct_g || params.ct_A || params.ct_G) {
        CUTADAPT(ch_fastq)
        ch_preprocessed_fastq = CUTADAPT.out.ch_cutadapt_fastq
    } else {
        ch_preprocessed_fastq = ch_fastq
    }

    emit:
    ch_preprocessed_fastq
}