include { FASTP }               from "$projectDir/modules/local/fastp/main"
include { FLASH2 }              from "$projectDir/modules/local/flash2/main"
include { BWA_SE; BWA_PE }      from "$projectDir/modules/local/bwa/main"
include { PICARD_DEDUP }        from "$projectDir/modules/local/picard/main"
include { MACS3_CALLPEAKS }     from "$projectDir/modules/local/macs3/main"
include {STARRPEAKER_CALLPEAKS} from "$projectDir/modules/local/starrpeaker/main"

workflow process_enhancer_lib {
    take:
    ch_enhancer

    main:
    ch_fastq = ch_enhancer.map { library, type, sample, replicate, read1, read2, reference ->
                                    tuple(library, type, sample, replicate, read1, read2) }
    
    /* -- remove duplicated reads -- */
    if (params.skip_dedup) {
        ch_dedup_fastq = ch_fastq
        ch_dedup_stats = Channel.empty()
    } else {
        FASTP(ch_fastq)
        ch_dedup_fastq = FASTP.out.ch_dedup_fastq
        ch_dedup_stats = FASTP.out.ch_dedup_stats
    }

    /* -- merge reads if needed and align reads -- */
    if (params.skip_flash2) {
        ch_align = ch_enhancer.map { library, type, sample, replicate, read1, read2, reference ->
                                    tuple(library, type, sample, replicate, reference) }
                              .join(ch_dedup_fastq, by: [0,1,2,3])
        
        BWA_PE{ch_align}
        ch_bam = BWA_PE.out.ch_bam
        ch_flagstat = BWA_PE.out.ch_flagstat

    } else {
        FLASH2(ch_dedup_fastq)
        ch_extended_frags = FLASH2.out.ch_extended_frags
        ch_not_combined = FLASH2.out.ch_not_combined
        ch_merge_stats = FLASH2.out.ch_merge_stats

        ch_align = ch_enhancer.map { library, type, sample, replicate, read1, read2, reference ->
                                    tuple(library, type, sample, replicate, reference) }
                              .join(ch_extended_frags, by: [0,1,2,3])

        BWA_SE{ch_align}
        ch_bam = BWA_SE.out.ch_bam
        ch_flagstat = BWA_SE.out.ch_flagstat
    }

    /* -- remove deduplicated reads by alignments -- */
    if (params.skip_dedup) {
        ch_picard_bam = ch_bam
        ch_picard_flagstat = Channel.empty()
    } else {
        PICARD_DEDUP(ch_bam)
        ch_picard_bam = PICARD_DEDUP.out.ch_picard_bam
        ch_picard_flagstat = PICARD_DEDUP.out.ch_picard_flagstat
    }

    /* -- call peaks -- */
    ch_picard_bam
        .branch {
            input: it[1] == "input"
            output: it[1] == "output"
        }
        .set { ch_picard_bam_by_type }
    
    ch_input_bam = ch_picard_bam_by_type.input.map { library, type, sample, replicate, bam, bai -> 
                                                    tuple(library, sample, bam, bai) }
    ch_output_bam = ch_picard_bam_by_type.output.map { library, type, sample, replicate, bam, bai -> 
                                                    tuple(library, sample, replicate, bam, bai) }
    
    /* -- macs3 -- */
    ch_macs3_sets = ch_output_bam.join(ch_input_bam, by: [0,1])
    MACS3_CALLPEAKS(ch_macs3_sets)
    ch_macs3_peaks = MACS3_CALLPEAKS.out.ch_macs3_peaks

    /* -- starrpeaker -- */
    ch_ref = ch_enhancer.map { library, type, sample, replicate, read1, read2, reference ->
                                tuple(library, reference) }
                        .unique()
    ch_starrpeaker_sets = ch_macs3_sets.join(ch_ref)

    ch_starrpeaker_sets = ch_starrpeaker_sets.filter { 
        library, sample, replicate, output_bam, output_bai, input_bam, input_bai, reference ->
        def starrpeaker_files = [
            "${params.resource}/starrpeaker/${reference}.chromsize.tsv",
            "${params.resource}/starrpeaker/${reference}.blacklist.bed",
            "${params.resource}/starrpeaker/${reference}.ucsc-gc-5bp.bw",
            "${params.resource}/starrpeaker/${reference}.gem-mappability-100mer.bw",
            "${params.resource}/starrpeaker/${reference}.linearfold-folding-energy-100bp.bw"
        ]

        def has_files = starrpeaker_files.every { file(it).exists() }
        if (!has_files) {
            log.warn "Skipping STARRPeaker for ${reference}: missing resource files"
        }

        return has_files
    }
    
    STARRPEAKER_CALLPEAKS(ch_starrpeaker_sets)
}
