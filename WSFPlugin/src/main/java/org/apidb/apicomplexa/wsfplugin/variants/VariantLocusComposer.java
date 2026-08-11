package org.apidb.apicomplexa.wsfplugin.variants;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Joins per-sample VCF calls to EDA metadata to produce the rows for the
 * "Strains / Samples" table.
 *
 * The join and aggregation logic here is the reusable part - it is done in Java, over
 * ~537 rows per locus, and nothing above it may reach into the VCF directly. {@link
 * #strainRows} itself is narrower: it returns presentation-ready table rows (see
 * {@link StrainRow}), not typed data. A future consumer that wants typed values -
 * a REST endpoint, say - should build on {@link LocusCalls} plus the metadata map,
 * or get its own method.
 */
public class VariantLocusComposer {

  public List<StrainRow> strainRows(LocusCalls locus, Map<String, String> countryBySample) {
    List<StrainRow> rows = new ArrayList<>();
    for (SampleCall c : locus.calls()) {
      String country = countryBySample.getOrDefault(c.sampleName(), "");
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
      String country = countryBySample.get(c.sampleName());
      if (country == null || country.isEmpty()) continue;
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
      if (total == 0) continue;

      final int denominator = total;
      List<String> ranked = new ArrayList<>(weights.keySet());
      ranked.sort((x, y) -> {
        int byWeight = Integer.compare(weights.get(y), weights.get(x));
        return byWeight != 0 ? byWeight : x.compareTo(y);   // deterministic tie-break
      });

      rows.add(new CountryRow(
          e.getKey(),
          calls.size(),
          formatted(ranked, weights, denominator, 0),
          formatted(ranked, weights, denominator, 1),
          formatted(ranked, weights, denominator, 2)));
    }

    // (-strainCount, country): the count is what a reader scans for, and the name
    // keeps ties off the VCF's arbitrary sample order.
    rows.sort((a, b) -> {
      int byCount = Integer.compare(b.strainCount(), a.strainCount());
      return byCount != 0 ? byCount : a.country().compareTo(b.country());
    });
    return rows;
  }

  private String formatted(List<String> ranked, Map<String, Integer> weights,
                           int denominator, int rank) {
    if (rank >= ranked.size()) return "";
    String allele = ranked.get(rank);
    return String.format(Locale.ROOT, "%s (%.4f)",
        allele, weights.get(allele) / (double) denominator);
  }
}
