package org.apidb.apicomplexa.wsfplugin.variants;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * One row per country. Empty - not an error - when the organism's dnaseq study carries no
 * country attribute; 14 of the 62 dnaseq studies do not, all of them lab lines or
 * reference assemblies. That case is caught by {@link #requiresSampleMetadata()}, which
 * lets the base class skip opening the VCF at all.
 */
public class VariantCountrySummaryPlugin extends AbstractVariantLocusTablePlugin {

  static final String[] DATA_COLUMNS =
      { "country", "strain_count", "major_allele", "minor_allele", "other_allele" };

  @Override
  protected String[] dataColumns() {
    return DATA_COLUMNS;
  }

  @Override
  protected String tableLabel() {
    return "country summary";
  }

  /** Every row of this table is keyed on a country, so no metadata means no table. */
  @Override
  protected boolean requiresSampleMetadata() {
    return true;
  }

  @Override
  protected List<String[]> dataCells(LocusCalls calls, Map<String, String> countryBySample) {
    List<String[]> cells = new ArrayList<>();
    for (CountryRow r : new VariantLocusComposer().countryRows(calls, countryBySample)) {
      // Cell order must track DATA_COLUMNS above.
      cells.add(new String[] {
          r.country(), String.valueOf(r.strainCount()),
          r.majorAlleleWithFrequency(), r.minorAlleleWithFrequency(),
          r.otherAlleleWithFrequency()
      });
    }
    return cells;
  }
}
