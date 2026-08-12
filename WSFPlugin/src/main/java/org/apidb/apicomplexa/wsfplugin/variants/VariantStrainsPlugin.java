package org.apidb.apicomplexa.wsfplugin.variants;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** One row per VCF sample at this variant's locus. */
public class VariantStrainsPlugin extends AbstractVariantLocusTablePlugin {

  static final String[] DATA_COLUMNS =
      { "strain", "country", "genotype", "allele", "depth", "read_frequency", "aa_product" };

  @Override
  protected String[] dataColumns() {
    return DATA_COLUMNS;
  }

  @Override
  protected String tableLabel() {
    return "strains table";
  }

  @Override
  protected List<String[]> dataCells(LocusCalls calls, Map<String, String> countryBySample) {
    List<String[]> cells = new ArrayList<>();
    for (StrainRow r : new VariantLocusComposer().strainRows(calls, countryBySample)) {
      // Cell order must track DATA_COLUMNS above.
      cells.add(new String[] {
          r.strain(), r.country(), r.genotype(), r.allele(),
          r.depth(), r.readFrequency(), r.aaProduct()
      });
    }
    return cells;
  }
}
