package org.apidb.apicomplexa.wsfplugin.variants;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Joins per-sample VCF calls to EDA metadata to produce the rows for both variant
 * record-page tables: {@link #strainRows} for "Strains / Samples" (one row per sample)
 * and {@link #countryRows} for "Country Summary" (one row per country, allele
 * frequencies aggregated over that country's samples).
 *
 * The join and aggregation logic here is the reusable part - it is done in Java, over
 * ~537 rows per locus, and nothing above it may reach into the VCF directly. Both
 * methods are narrower than that, though: they return presentation-ready table rows
 * (see {@link StrainRow} and {@link CountryRow}), with numbers already formatted into
 * strings, not typed data. A future consumer that wants typed values - a REST endpoint,
 * say - should build on {@link LocusCalls} plus the metadata map, or get its own method,
 * rather than parsing these cells back apart.
 */
public class VariantLocusComposer {

  public List<StrainRow> strainRows(LocusCalls locus, Map<String, String> countryBySample) {
    List<StrainRow> rows = new ArrayList<>();
    for (SampleCall c : locus.calls()) {
      String country = countryOf(c, countryBySample);
      if (c.noCall()) {
        // Present, not omitted: the record advertises no_call_strain_count, so a table
        // that hid these would contradict the overview panel above it. The genotype
        // column is hidden on the page (internal="true" on the WDK columnAttribute),
        // so "No call" must show in the Allele column instead - otherwise a no-call
        // row is visually indistinguishable from missing data.
        rows.add(new StrainRow(c.sampleName(), country, "No call", "No call", "", "", ""));
        continue;
      }
      rows.add(new StrainRow(
          c.sampleName(),
          country,
          c.genotype(),
          c.allele(),
          c.depth() == null ? "" : String.valueOf(c.depth()),
          c.readFrequency() == null ? "" : c.readFrequency(),
          String.join(", ", c.aminoAcids())));
    }
    return rows;
  }

  /**
   * One row per country, over samples that HAVE a country. Samples without one are
   * excluded entirely, as is the reference strain, which has no collection site in EDA.
   *
   * Frequencies are ploidy-weighted (one unit per chromosome slot, denominator = the
   * country's own chromosome count) per processSequenceVariations.jl's
   * aggregate_locus_alleles. They therefore do NOT match the locus-wide
   * snp_major_allele_frequency in the overview panel; that is deliberate, and the column
   * help says so.
   */
  public List<CountryRow> countryRows(LocusCalls locus, Map<String, String> countryBySample) {
    Map<String, List<SampleCall>> byCountry = new LinkedHashMap<>();
    for (SampleCall c : locus.calls()) {
      if (c.noCall()) continue;
      String country = countryOf(c, countryBySample);
      if (country.isEmpty()) continue;
      byCountry.computeIfAbsent(country, k -> new ArrayList<>()).add(c);
    }

    List<CountryRow> rows = new ArrayList<>();
    for (Map.Entry<String, List<SampleCall>> e : byCountry.entrySet()) {
      List<SampleCall> calls = e.getValue();

      Map<String, Integer> weights = new LinkedHashMap<>();
      int total = 0;
      for (SampleCall c : calls) {
        for (String a : c.chromosomeAlleles()) {
          weights.merge(a, 1, Integer::sum);
          total++;
        }
      }
      // A country whose samples contribute no chromosome slots at all disappears from
      // the table rather than rendering a row of blank cells. Preferred: a row with a
      // strain count but no frequencies reads as a data bug to anyone looking at it.
      if (total == 0) continue;

      List<Map.Entry<String, Integer>> ranked = new ArrayList<>(weights.entrySet());
      ranked.sort(Map.Entry.<String, Integer>comparingByValue().reversed()
          .thenComparing(Map.Entry.comparingByKey()));   // name tie-break keeps it deterministic

      rows.add(new CountryRow(
          e.getKey(),
          calls.size(),
          alleleCell(ranked, 0, total),
          // Ranks beyond 3 are dropped: the table has three allele columns by design,
          // and at a biallelic SNP - the overwhelming majority - rank 3 is already "".
          alleleCell(ranked, 1, total),
          alleleCell(ranked, 2, total)));
    }

    // (-strainCount, country): the count is what a reader scans for, and the name
    // keeps ties off the VCF's arbitrary sample order.
    rows.sort((a, b) -> {
      int byCount = Integer.compare(b.strainCount(), a.strainCount());
      return byCount != 0 ? byCount : a.country().compareTo(b.country());
    });
    return rows;
  }

  /** Trimmed country, or "" when the sample has none. The two row builders must agree. */
  private static String countryOf(SampleCall c, Map<String, String> countryBySample) {
    String v = countryBySample.get(c.sampleName());
    return v == null ? "" : v.trim();
  }

  /** "{@code <allele> (<freq>)}" for the rank-th allele, or "" when there is no such rank. */
  private static String alleleCell(List<Map.Entry<String, Integer>> ranked, int rank,
                                   int denominator) {
    if (rank >= ranked.size()) return "";
    Map.Entry<String, Integer> e = ranked.get(rank);
    return String.format(Locale.ROOT, "%s (%.4f)",
        e.getKey(), e.getValue() / (double) denominator);
  }
}
