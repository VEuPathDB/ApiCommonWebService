package org.apidb.apicomplexa.wsfplugin.variants;

public record StrainRow(
    String strain,
    String country,
    String genotype,
    String allele,
    String depth,
    String readFrequency,
    String aaProduct
) {}
