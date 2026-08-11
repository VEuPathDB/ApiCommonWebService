package org.apidb.apicomplexa.wsfplugin.variants;

public record CountryRow(
    String country,
    int strainCount,
    String majorAllele,
    String minorAllele,
    String otherAllele
) {}
