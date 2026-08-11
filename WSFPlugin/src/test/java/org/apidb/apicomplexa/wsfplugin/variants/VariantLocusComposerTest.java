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
    return new LocusCalls("chr1", 100, "T", List.of("C"), CannIndex.parse("."), calls);
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
    assertEquals("", rows.get(1).allele());
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
}
