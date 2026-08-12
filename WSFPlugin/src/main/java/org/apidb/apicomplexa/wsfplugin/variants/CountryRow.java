package org.apidb.apicomplexa.wsfplugin.variants;

/**
 * One country's row in the "Country Summary" table. Presentation-ready: the allele
 * fields are rendered cells, not values - "{@code C (0.3333)}", allele then the
 * frequency to four decimal places. A rank with no allele is the empty string, so a
 * biallelic locus - the usual case - leaves {@code otherAlleleWithFrequency} blank.
 * Alleles are ranked by descending weight, ties broken by allele name.
 *
 * Note the two different units in one row: {@code strainCount} counts SAMPLES, while
 * the frequencies are chromosome-weighted (one unit per chromosome slot, denominator =
 * this country's own chromosome count). For three diploid strains, "0.6667" is 4/6, not
 * 2/3, and the two only coincide because P. falciparum is haploid. Frequencies are also
 * per-country by design, so they do NOT match the locus-wide
 * snp_major_allele_frequency in the record's overview panel.
 *
 * @param majorAlleleWithFrequency rank 1, rendered - e.g. "T (0.6667)"
 * @param minorAlleleWithFrequency rank 2, rendered, or "" if the locus is monomorphic here
 * @param otherAlleleWithFrequency rank 3, rendered, or "" - ranks beyond 3 are not shown
 */
public record CountryRow(
    String country,
    int strainCount,
    String majorAlleleWithFrequency,
    String minorAlleleWithFrequency,
    String otherAlleleWithFrequency
) {}
