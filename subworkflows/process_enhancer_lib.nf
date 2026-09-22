include { FASTP }                  from "$projectDir/modules/local/fastp/main"
include { FLASH2 }                 from "$projectDir/modules/local/flash2/main"
include { BOWTIE2_SE; BOWTIE2_PE } from "$projectDir/modules/local/bowtie2/main"
include { BWA_SE; BWA_PE }         from "$projectDir/modules/local/bwa/main"
include { PICARD_DEDUP }           from "$projectDir/modules/local/picard/main"
include { BASIC_STATS }            from "$projectDir/modules/local/basic_stats/main"
include { BAMCOVERAGE }            from "$projectDir/modules/local/bamCoverage/main"
include { BAMCOMPARE }             from "$projectDir/modules/local/bamCompare/main"
include { MACS3_CALLPEAKS }        from "$projectDir/modules/local/macs3/main"
include { STARRPEAKER_CALLPEAKS }  from "$projectDir/modules/local/starrpeaker/main"

workflow process_enhancer_lib {
    take:
    ch_enhancer

    main:
    ch_fastq = ch_enhancer.map { library, type, sample, replicate, read1, read2, reference, blacklist ->
                                    tuple(library, type, sample, replicate, read1, read2) }
    
    // -------------------------------------------------
    // remove duplicated reads
    // -------------------------------------------------
    FASTP(ch_fastq)
    ch_dedup_fastq = FASTP.out.ch_dedup_fastq
    ch_dedup_stats = FASTP.out.ch_dedup_stats
    
    // -------------------------------------------------
    // merge reads if needed and align reads
    // -------------------------------------------------
    if (params.skip_flash2) {
        ch_align = ch_enhancer.map { library, type, sample, replicate, read1, read2, reference, blacklist ->
                                    tuple(library, type, sample, replicate, reference) }
                              .join(ch_dedup_fastq, by: [0,1,2,3])
        
        if (params.aligner == "bowtie2") {
            BOWTIE2_PE{ch_align}
            ch_bam = BOWTIE2_PE.out.ch_bam
            ch_flagstat = BOWTIE2_PE.out.ch_flagstat
        } else {
            BWA_PE{ch_align}
            ch_bam = BWA_PE.out.ch_bam
            ch_flagstat = BWA_PE.out.ch_flagstat
        }
    } else {
        FLASH2(ch_dedup_fastq)
        ch_extended_frags = FLASH2.out.ch_extended_frags
        ch_not_combined = FLASH2.out.ch_not_combined
        ch_merge_stats = FLASH2.out.ch_merge_stats

        ch_align = ch_enhancer.map { library, type, sample, replicate, read1, read2, reference, blacklist ->
                                    tuple(library, type, sample, replicate, reference) }
                              .join(ch_extended_frags, by: [0,1,2,3])

        if (params.aligner == "bowtie2") {
            BOWTIE2_SE(ch_align)
            ch_bam = BOWTIE2_SE.out.ch_bam
            ch_flagstat = BOWTIE2_SE.out.ch_flagstat
        } else {
            BWA_SE{ch_align}
            ch_bam = BWA_SE.out.ch_bam
            ch_flagstat = BWA_SE.out.ch_flagstat
        }
    }

    // -------------------------------------------------
    // remove deduplicated reads by alignments
    // -------------------------------------------------
    PICARD_DEDUP(ch_bam)
    ch_picard_bam = PICARD_DEDUP.out.ch_picard_bam
    ch_picard_flagstat = PICARD_DEDUP.out.ch_picard_flagstat

    // -------------------------------------------------
    // generate basic stats and figures
    // -------------------------------------------------
    ch_basic_stats = ch_dedup_stats.join(ch_flagstat, by: [0,1,2,3])
                                   .join(ch_picard_flagstat, by: [0,1,2,3])
    BASIC_STATS(ch_basic_stats)
    ch_basic_stats_outs = BASIC_STATS.out.ch_basic_stats_outs
  
    // -------------------------------------------------
    // convert BAM to bigwig
    // -------------------------------------------------
    BAMCOVERAGE(ch_picard_bam)

    // -------------------------------------------------
    // split samples by input and output
    // -------------------------------------------------
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

    // -------------------------------------------------
    // calculate bigwig log2 ratio
    // -------------------------------------------------
    ch_paired_sets = ch_output_bam.combine(ch_input_bam, by: [0,1])
    BAMCOMPARE(ch_paired_sets)

    // -------------------------------------------------
    // create callpeaks inputs
    // -------------------------------------------------
    ch_blacklist = ch_enhancer.map { library, type, sample, replicate, read1, read2, reference, blacklist -> 
                                    tuple(library, blacklist) }
                              .unique()
    ch_ref = ch_enhancer.map { library, type, sample, replicate, read1, read2, reference, blacklist -> 
                                tuple(library, reference) }
                        .unique()  
    ch_callpeak_inputs = ch_output_bam.combine(ch_input_bam, by: [0,1])
                                      .combine(ch_blacklist, by: [0])
                                      .combine(ch_ref, by: [0])

    ch_callpeak_inputs = ch_callpeak_inputs
        .filter {
            library, sample, replicate, output_bam, output_bai, input_bam, input_bai, blacklist, reference ->
            def has_user_file = blacklist && blacklist.trim() && file(blacklist).exists()
            def default_file = file("${params.resource}/starrpeaker/${reference}.blacklist.bed")
            has_user_file || default_file.exists()
        }
        .map {
            library, sample, replicate, output_bam, output_bai, input_bam, input_bai, blacklist, reference ->
            def has_user_file = blacklist && blacklist.trim() && file(blacklist).exists()
            def default_file = file("${params.resource}/starrpeaker/${reference}.blacklist.bed")
            def selected_file = has_user_file ? file(blacklist) : default_file
            tuple(library, sample, replicate, output_bam, output_bai, input_bam, input_bai, selected_file, reference)
        }


    // -------------------------------------------------
    // callpeaks macs3
    // -------------------------------------------------
    MACS3_CALLPEAKS(ch_callpeak_inputs)
    ch_macs3_peaks = MACS3_CALLPEAKS.out.ch_macs3_peaks

    // -------------------------------------------------
    // callpeaks starrpeaker
    // -------------------------------------------------
    ch_callpeak_inputs = ch_callpeak_inputs.filter { 
        library, sample, replicate, output_bam, output_bai, input_bam, input_bai, blacklist, reference ->
        def starrpeaker_files = [
            "${params.resource}/starrpeaker/${reference}.chromsize.tsv",
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

    STARRPEAKER_CALLPEAKS(ch_callpeak_inputs)
    ch_starrpeaker_peaks = STARRPEAKER_CALLPEAKS.out.ch_starrpeaker_peaks

    emit:
    ch_basic_stats_outs
}
