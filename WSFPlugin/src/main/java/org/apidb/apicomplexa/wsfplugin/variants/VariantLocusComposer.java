package org.apidb.apicomplexa.wsfplugin.variants;

import java.util.ArrayList;
import java.util.List;
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
        // that hid these would contradict the overview panel above it.
        rows.add(new StrainRow(c.sampleName(), country, "No call", "", "", "", ""));
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
}
