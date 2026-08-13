package org.apidb.apicomplexa.wsfplugin.variants;

import java.util.List;

/**
 * One sample's call at one locus. A single row regardless of ploidy: a diploid
 * het collapses into an IUPAC ambiguity code in {@code allele}.
 *
 * @param allele            display form - IUPAC for a het. NOT for aggregating.
 * @param chromosomeAlleles one entry per chromosome slot, duplicates kept. This is
 *                          the aggregation weight; a diploid het has two entries.
 * @param readFrequency     support for the CALLED allele as a percentage string, or
 *                          null when unknown. See MergedVcfReader for the definition.
 */
public record SampleCall(
    String sampleName,
    String genotype,
    Integer depth,
    String allele,
    String readFrequency,
    List<String> aminoAcids,
    List<String> chromosomeAlleles,
    int ploidy,
    boolean noCall,
    boolean coverageFilled
) {}
