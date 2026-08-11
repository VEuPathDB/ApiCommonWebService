package org.apidb.apicomplexa.wsfplugin.variants;

import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class VariantLocusComposerTest {

  private static SampleCall call(String name, String gt, String allele, int ploidy,
                                 String freq, List<String> aas) {
    return new SampleCall(name, gt, 10, allele, freq, aas,
        Collections.nCopies(ploidy, allele), ploidy, false, false);
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
    LocusCalls l = locus(List.of(
        call("S1", "1", "C", 1, "80.00", List.of("A")),
        call("S2", "0", "T", 1, "100.00", List.of("V")),
        call("S3", "0", "T", 1, "100.00", List.of("V")),
        call("S4", "1", "C", 1, "90.00", List.of("A"))));

    List<CountryRow> rows = new VariantLocusComposer().countryRows(l,
        Map.of("S1", "Mali", "S2", "Mali", "S3", "Mali", "S4", "Gambia"));

    assertEquals(2, rows.size());
    CountryRow mali = rows.get(0);
    assertEquals("Mali", mali.country());
    assertEquals(3, mali.strainCount());
    assertEquals("T (0.6667)", mali.majorAllele());
    assertEquals("C (0.3333)", mali.minorAllele());
    assertEquals("", mali.otherAllele());
  }

  @Test
  public void countryRowsExcludeSamplesWithNoCountryAndNoCalls() {
    LocusCalls l = locus(List.of(
        call("S1", "1", "C", 1, "80.00", List.of("A")),
        call("S2", "0", "T", 1, "100.00", List.of("V")),
        noCall("S3")));

    // S2 has no country; S3 is a no-call with one.
    List<CountryRow> rows = new VariantLocusComposer().countryRows(l,
        Map.of("S1", "Mali", "S3", "Mali"));

    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).strainCount());
    assertEquals("C (1.0000)", rows.get(0).majorAllele());
  }

  @Test
  public void diploidHetContributesOneUnitToEachAllele() {
    // allele() is the IUPAC display value; chromosomeAlleles() is what aggregation uses.
    LocusCalls l = locus(List.of(
        new SampleCall("S1", "0/1", 20, "Y", "50.00", List.of(),
            List.of("T", "C"), 2, false, false)));
    List<CountryRow> rows = new VariantLocusComposer()
        .countryRows(l, Map.of("S1", "Mali"));

    assertEquals(1, rows.size());
    assertEquals(1, rows.get(0).strainCount());
    // Two chromosomes, one T and one C -> 0.5 each.
    assertEquals("C (0.5000)", rows.get(0).majorAllele());
    assertEquals("T (0.5000)", rows.get(0).minorAllele());
  }

  @Test
  public void noCountryAttributeYieldsNoRows() {
    LocusCalls l = locus(List.of(call("S1", "1", "C", 1, "80.00", List.of("A"))));
    assertEquals(List.of(), new VariantLocusComposer().countryRows(l, Map.of()));
  }
}
