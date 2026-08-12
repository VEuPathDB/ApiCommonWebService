package org.apidb.apicomplexa.wsfplugin.variants;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class VariantLocusComposerTest {

  private static SampleCall call(String name, String gt, String allele, int ploidy,
                                 String freq, List<String> aas) {
    return new SampleCall(name, gt, 10, allele, freq, aas,
        Collections.nCopies(ploidy, allele), ploidy, false, false);
  }

  /** Haploid call; genotype/frequency/AA are irrelevant to country aggregation. */
  private static SampleCall haploid(String name, String allele) {
    return call(name, "1", allele, 1, "100.00", List.of());
  }

  private static SampleCall noCall(String name) {
    return new SampleCall(name, ".", null, "", null, List.of(), List.of(), 0, true, false);
  }

  private static LocusCalls locus(List<SampleCall> calls) {
    return new LocusCalls("chr1", 100, "T", List.of("C"), calls);
  }

  @Test
  public void strainRowsKeepEverySampleIncludingNoCalls() {
    LocusCalls l = locus(List.of(
        call("S1", "1", "C", 1, "80.00", List.of("A")),
        noCall("S2")));
    List<StrainRow> rows = new VariantLocusComposer()
        .strainRows(l, Map.of("S1", "Mali"));

    assertEquals(2, rows.size());
    assertEquals("Mali", rows.get(0).country());
    assertEquals("C", rows.get(0).allele());
    assertEquals("A", rows.get(0).aaProduct());

    assertEquals("No call", rows.get(1).genotype());
    // Genotype is hidden on the page (see variantRecords.xml), so a no-call row
    // must show "No call" in the Allele column too, or it looks like missing data.
    assertEquals("No call", rows.get(1).allele());
    assertEquals("", rows.get(1).country());
  }

  @Test
  public void multipleTranscriptAminoAcidsAreCommaJoined() {
    LocusCalls l = locus(List.of(call("S1", "1", "C", 1, "80.00", List.of("A", "M"))));
    StrainRow row = new VariantLocusComposer().strainRows(l, Map.of()).get(0);
    assertEquals("A, M", row.aaProduct());
    assertEquals("", row.country());
  }

  @Test
  public void calledSampleWithNullDepthAndReadFrequencyRendersEmptyStrings() {
    SampleCall c = new SampleCall("S1", "1", null, "C", null, List.of("A"),
        List.of("C"), 1, false, false);
    LocusCalls l = locus(List.of(c));
    StrainRow row = new VariantLocusComposer().strainRows(l, Map.of()).get(0);

    assertEquals("1", row.genotype());
    assertEquals("C", row.allele());
    assertEquals("", row.depth());
    assertEquals("", row.readFrequency());
  }

  @Test
  public void countryRowsAreWeightedByChromosomeAndSortedByCount() {
    // Gambia's single sample is encountered FIRST, so grouping order alone would put
    // it at index 0. Only the row sort moves Mali ahead of it - delete the sort and
    // this test goes red.
    LocusCalls l = locus(List.of(
        haploid("S4", "C"), haploid("S1", "C"), haploid("S2", "T"), haploid("S3", "T")));

    List<CountryRow> rows = new VariantLocusComposer().countryRows(l,
        Map.of("S1", "Mali", "S2", "Mali", "S3", "Mali", "S4", "Gambia"));

    assertEquals(2, rows.size());
    CountryRow mali = rows.get(0);
    assertEquals("Mali", mali.country());
    assertEquals(3, mali.strainCount());
    assertEquals("T (0.6667)", mali.majorAlleleWithFrequency());
    assertEquals("C (0.3333)", mali.minorAlleleWithFrequency());
    assertEquals("", mali.otherAlleleWithFrequency());

    assertEquals("Gambia", rows.get(1).country());
    assertEquals(1, rows.get(1).strainCount());
  }

  @Test
  public void equalStrainCountsTieBreakAlphabeticallyByCountry() {
    // Encountered Zambia-then-Angola; both have one strain, so only the name
    // tie-break decides the order.
    LocusCalls l = locus(List.of(haploid("S1", "C"), haploid("S2", "T")));

    List<CountryRow> rows = new VariantLocusComposer().countryRows(l,
        Map.of("S1", "Zambia", "S2", "Angola"));

    assertEquals(2, rows.size());
    assertEquals("Angola", rows.get(0).country());
    assertEquals("Zambia", rows.get(1).country());
  }

  @Test
  public void thirdRankedAlleleRendersInTheOtherColumn() {
    // Six haploid chromosomes in one country: A x3, C x2, G x1.
    LocusCalls l = locus(List.of(
        haploid("S1", "A"), haploid("S2", "A"), haploid("S3", "A"),
        haploid("S4", "C"), haploid("S5", "C"), haploid("S6", "G")));

    List<CountryRow> rows = new VariantLocusComposer().countryRows(l,
        Map.of("S1", "Mali", "S2", "Mali", "S3", "Mali",
               "S4", "Mali", "S5", "Mali", "S6", "Mali"));

    assertEquals(1, rows.size());
    CountryRow mali = rows.get(0);
    assertEquals(6, mali.strainCount());
    assertEquals("A (0.5000)", mali.majorAlleleWithFrequency());
    assertEquals("C (0.3333)", mali.minorAlleleWithFrequency());
    assertEquals("G (0.1667)", mali.otherAlleleWithFrequency());
  }

  @Test
  public void fourthRankedAlleleIsDroppedNotShown() {
    // A x4, C x3, G x2, T x1 over ten haploid chromosomes. The table has three allele
    // columns, so T is dropped entirely - the row does not sum to 1.0000, by design.
    LocusCalls l = locus(List.of(
        haploid("S1", "A"), haploid("S2", "A"), haploid("S3", "A"), haploid("S4", "A"),
        haploid("S5", "C"), haploid("S6", "C"), haploid("S7", "C"),
        haploid("S8", "G"), haploid("S9", "G"),
        haploid("S10", "T")));

    Map<String, String> countries = new HashMap<>();
    for (int i = 1; i <= 10; i++) countries.put("S" + i, "Mali");

    List<CountryRow> rows = new VariantLocusComposer().countryRows(l, countries);

    assertEquals(1, rows.size());
    CountryRow mali = rows.get(0);
    assertEquals(10, mali.strainCount());
    assertEquals("A (0.4000)", mali.majorAlleleWithFrequency());
    assertEquals("C (0.3000)", mali.minorAlleleWithFrequency());
    assertEquals("G (0.2000)", mali.otherAlleleWithFrequency());
    // The rank-4 allele appears in no column at all.
    assertFalse(mali.majorAlleleWithFrequency().startsWith("T"));
    assertFalse(mali.minorAlleleWithFrequency().startsWith("T"));
    assertFalse(mali.otherAlleleWithFrequency().startsWith("T"));
  }

  @Test
  public void countryRowsExcludeSamplesWithNoCountryAndNoCalls() {
    LocusCalls l = locus(List.of(haploid("S1", "C"), haploid("S2", "T"), noCall("S3")));

    // S2 has no country; S3 is a no-call with one.
    List<CountryRow> rows = new VariantLocusComposer().countryRows(l,
        Map.of("S1", "Mali", "S3", "Mali"));

    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).strainCount());
    assertEquals("C (1.0000)", rows.get(0).majorAlleleWithFrequency());
  }

  @Test
  public void blankCountryIsTreatedAsNoCountry() {
    // SampleMetadataLookup's SQL rejects NULL but not blank, and EDA free text is not
    // normalised anywhere, so "  " must not become its own country group.
    LocusCalls l = locus(List.of(haploid("S1", "C"), haploid("S2", "T")));

    Map<String, String> countries = new HashMap<>();
    countries.put("S1", "Mali");
    countries.put("S2", "  ");

    List<CountryRow> rows = new VariantLocusComposer().countryRows(l, countries);

    assertEquals(1, rows.size());
    assertEquals("Mali", rows.get(0).country());
    assertEquals(1, rows.get(0).strainCount());
  }

  @Test
  public void countryNamesAreTrimmedBeforeGrouping() {
    // Untrimmed EDA text would otherwise split one country across two rows.
    LocusCalls l = locus(List.of(haploid("S1", "C"), haploid("S2", "T")));

    Map<String, String> countries = new HashMap<>();
    countries.put("S1", "Mali ");
    countries.put("S2", "Mali");

    List<CountryRow> rows = new VariantLocusComposer().countryRows(l, countries);

    assertEquals(1, rows.size());
    assertEquals("Mali", rows.get(0).country());
    assertEquals(2, rows.get(0).strainCount());
  }

  @Test
  public void strainRowsTrimTheCountryToo() {
    // Same rule as countryRows, or the two tables disagree on the same sample.
    LocusCalls l = locus(List.of(haploid("S1", "C")));

    Map<String, String> countries = new HashMap<>();
    countries.put("S1", " Mali ");

    assertEquals("Mali",
        new VariantLocusComposer().strainRows(l, countries).get(0).country());
  }

  @Test
  public void diploidHetContributesOneUnitToEachAllele() {
    // allele() is the IUPAC display value; chromosomeAlleles() is what aggregation uses.
    // This is also the ONLY test covering the equal-weight allele tie-break - C before T
    // at 0.5 each - so do not weaken it to a single-allele case.
    LocusCalls l = locus(List.of(
        new SampleCall("S1", "0/1", 20, "Y", "50.00", List.of(),
            List.of("T", "C"), 2, false, false)));
    List<CountryRow> rows = new VariantLocusComposer()
        .countryRows(l, Map.of("S1", "Mali"));

    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).strainCount());
    // Two chromosomes, one T and one C -> 0.5 each.
    assertEquals("C (0.5000)", rows.get(0).majorAlleleWithFrequency());
    assertEquals("T (0.5000)", rows.get(0).minorAlleleWithFrequency());
  }

  @Test
  public void noCountryAttributeYieldsNoRows() {
    LocusCalls l = locus(List.of(haploid("S1", "C")));
    assertEquals(List.of(), new VariantLocusComposer().countryRows(l, Map.of()));
  }
}
