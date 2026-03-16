#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

include { starr_seq } from './workflows/starr_seq.nf'

workflow {
    starr_seq()
}
