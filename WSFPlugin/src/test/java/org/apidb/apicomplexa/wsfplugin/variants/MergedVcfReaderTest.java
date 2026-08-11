package org.apidb.apicomplexa.wsfplugin.variants;

import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MergedVcfReaderTest {

  private Map<String, SampleCall> callsAt(String seq, int pos) {
    Path vcf = Paths.get("src/test/resources/variants/fixture.vcf.gz");
    try (MergedVcfReader reader = new MergedVcfReader(vcf)) {
      LocusCalls locus = reader.read(seq, pos).orElseThrow();
      assertEquals("T", locus.refAllele());
      return locus.calls().stream()
          .collect(Collectors.toMap(SampleCall::sampleName, Function.identity()));
    }
  }

  @Test
  public void altCallCarriesAlleleDepthAndReadFrequency() {
    SampleCall c = callsAt("chr1", 100).get("S_ALT");
    assertEquals("1", c.genotype());
    assertEquals("C", c.allele());
    assertEquals(Integer.valueOf(5), c.depth());
    assertEquals("80.00", c.readFrequency());   // AO 4 / (RO 1 + AO 4)
    assertFalse(c.noCall());
  }

  @Test
  public void coverageFilledReferenceCallReportsFullSupport() {
    SampleCall c = callsAt("chr1", 100).get("S_FILLED");
    assertEquals("T", c.allele());
    assertEquals(Integer.valueOf(128), c.depth());
    assertTrue(c.coverageFilled());
    assertEquals("100.00", c.readFrequency());
  }

  /**
   * The reader -> CannIndex seam that was missing before this fix: CANN is
   * Number=. in the header, so htsjdk hands the reader a List<String>, not a
   * String. This asserts on aminoAcids() coming out of the actual reader (not a
   * hand-built SampleCall, and not CannIndexTest's direct CannIndex.parse(String)
   * call), so a regression to round-tripping the attribute through toString()
   * would fail here even if CannIndexTest still passes.
   */
  @Test
  public void aminoAcidsResolveThroughTheActualReaderNotJustCannIndex() {
    Map<String, SampleCall> calls = callsAt("chr1", 100);
    // S_ALT: CA=k0 -> missense entry k0, amino acid A.
    assertEquals(List.of("A"), calls.get("S_ALT").aminoAcids());
    // S_FILLED: CA=r0 -> reference entry r0, amino acid V.
    assertEquals(List.of("V"), calls.get("S_FILLED").aminoAcids());
  }

  @Test
  public void realReferenceCallReportsReferenceSupport() {
    SampleCall c = callsAt("chr1", 100).get("S_REFREAL");
    assertEquals("T", c.allele());
    assertFalse(c.coverageFilled());
    assertEquals("80.00", c.readFrequency());   // RO 8 / (RO 8 + AO 2)
  }

  @Test
  public void noCallIsPresentButEmpty() {
    SampleCall c = callsAt("chr1", 100).get("S_NOCALL");
    assertTrue(c.noCall());
    assertEquals("", c.allele());
    assertNull(c.depth());
    assertNull(c.readFrequency());
  }

  @Test
  public void diploidHetCollapsesToIupacOnOneRow() {
    SampleCall c = callsAt("chr1", 100).get("S_DIPHET");
    assertEquals("Y", c.allele());              // T + C
    assertEquals("0/1", c.genotype());
    assertEquals(2, c.ploidy());
  }

  @Test
  public void chromosomeAllelesKeepOneEntryPerSlot() {
    Map<String, SampleCall> calls = callsAt("chr1", 100);
    // The aggregation weight: duplicates are kept, IUPAC is NOT used here.
    assertEquals(List.of("T", "C"), calls.get("S_DIPHET").chromosomeAlleles());
    assertEquals(List.of("C"), calls.get("S_ALT").chromosomeAlleles());
    assertEquals(List.of(), calls.get("S_NOCALL").chromosomeAlleles());
  }

  @Test
  public void multiAllelicAltCallUsesIndexedAoNotSum() {
    // chr1:200 REF A, ALT G,T. Sample called for the SECOND alt (T, index 2), with
    // AO=3,9 for alt1,alt2 respectively and RO=1. Indexed: 9/(1+9) = 90.00.
    // Summing (the bug) would give (3+9)/(1+3+9) = 92.31 instead - a visibly
    // different number, so this fails loudly if the fix regresses.
    Path vcf = Paths.get("src/test/resources/variants/fixture.vcf.gz");
    try (MergedVcfReader reader = new MergedVcfReader(vcf)) {
      LocusCalls locus = reader.read("chr1", 200).orElseThrow();
      assertEquals("A", locus.refAllele());
      SampleCall c = locus.calls().stream()
          .collect(Collectors.toMap(SampleCall::sampleName, Function.identity()))
          .get("S_ALT");
      assertEquals("2", c.genotype());
      assertEquals("T", c.allele());
      assertEquals("90.00", c.readFrequency());
    }
  }

  @Test
  public void outOfRangeAoIndexFallsBackToSum() {
    // chr1:300 REF A, ALT G,T,C. Sample called for the THIRD alt (C, index 3),
    // but AO="4,7" reports only two slots - alt3's own AO was never recorded.
    // idx = 3-1 = 2 is out of range for a length-2 array, so this falls back to
    // the summed real AO: (4+7)/(2+4+7) = 11/13 = 84.62.
    Path vcf = Paths.get("src/test/resources/variants/fixture.vcf.gz");
    try (MergedVcfReader reader = new MergedVcfReader(vcf)) {
      LocusCalls locus = reader.read("chr1", 300).orElseThrow();
      SampleCall c = locus.calls().stream()
          .collect(Collectors.toMap(SampleCall::sampleName, Function.identity()))
          .get("S_ALT");
      assertEquals("3", c.genotype());
      assertEquals("C", c.allele());
      assertEquals("84.62", c.readFrequency());
    }
  }

  @Test
  public void inRangeSentinelAtCalledIndexFallsBackToSumRatherThanNegative() {
    // chr1:600 REF A, ALT G,T,C. AO="3,.,9" is the CORRECT length for 3 alts, and
    // the sample is called for alt2 (idx = 2-1 = 1, in range) - whose own slot is
    // the '.'. Without the sentinel check this would read ao[1] == MISSING (-1)
    // straight into the percentage. It must instead fall back to the summed real
    // AO: (3+9)/(5+3+9) = 12/17 = 70.59.
    Path vcf = Paths.get("src/test/resources/variants/fixture.vcf.gz");
    try (MergedVcfReader reader = new MergedVcfReader(vcf)) {
      LocusCalls locus = reader.read("chr1", 600).orElseThrow();
      SampleCall c = locus.calls().stream()
          .collect(Collectors.toMap(SampleCall::sampleName, Function.identity()))
          .get("S_ALT");
      assertEquals("2", c.genotype());
      assertEquals("T", c.allele());
      assertEquals("70.59", c.readFrequency());
    }
  }

  @Test
  public void complexHetSlashJoinsDistinctAllelesNotIupac() {
    // chr1:400 REF A, ALT AG (multi-base) - a het of A/AG isn't two single-base
    // alleles, so it can't collapse to an IUPAC code; the unified allele-display
    // rule slash-joins its distinct chromosome alleles instead ("A/AG"), the same
    // "otherwise" branch a multi-record locus falls into (see
    // twoContributingRecordsSlashJoinDistinctAlleles).
    Path vcf = Paths.get("src/test/resources/variants/fixture.vcf.gz");
    try (MergedVcfReader reader = new MergedVcfReader(vcf)) {
      LocusCalls locus = reader.read("chr1", 400).orElseThrow();
      SampleCall c = locus.calls().stream()
          .collect(Collectors.toMap(SampleCall::sampleName, Function.identity()))
          .get("S_ALT");
      assertEquals("0/1", c.genotype());
      assertEquals("A/AG", c.allele());
    }
  }

  @Test
  public void realHomRefAtMultiAllelicLocusSumsBothAoEntries() {
    // chr1:500 REF A, ALT G,T. Sample is a REAL (not coverage-filled) hom-ref
    // call with two nonzero AO entries: RO=10, AO=3,5. Reference support sums
    // every non-ref read regardless of which alt it supports: 10/(10+3+5) = 55.56.
    Path vcf = Paths.get("src/test/resources/variants/fixture.vcf.gz");
    try (MergedVcfReader reader = new MergedVcfReader(vcf)) {
      LocusCalls locus = reader.read("chr1", 500).orElseThrow();
      SampleCall c = locus.calls().stream()
          .collect(Collectors.toMap(SampleCall::sampleName, Function.identity()))
          .get("S_ALT");
      assertEquals("0", c.genotype());
      assertEquals("A", c.allele());
      assertFalse(c.coverageFilled());
      assertEquals("55.56", c.readFrequency());
    }
  }

  @Test
  public void absentLocusIsEmptyNotAnError() {
    Path vcf = Paths.get("src/test/resources/variants/fixture.vcf.gz");
    try (MergedVcfReader reader = new MergedVcfReader(vcf)) {
      assertTrue(reader.read("chr1", 999).isEmpty());
    }
  }

  /**
   * chr1:700 is a multi-record locus - two records, same CHROM/POS/REF (A), different
   * ALTs (AT, ATT) with their own CANN, mirroring the real merged.ann.vcf.gz shape
   * (measured: 9,384 of 47,189 positions, 19.9%, have more than one record; one had
   * seven, all sharing REF=A with different indel ALTs). altAlleles must be the union
   * across records, in record order.
   */
  private Map<String, SampleCall> multiRecordCalls() {
    Path vcf = Paths.get("src/test/resources/variants/fixture.vcf.gz");
    try (MergedVcfReader reader = new MergedVcfReader(vcf)) {
      LocusCalls locus = reader.read("chr1", 700).orElseThrow();
      assertEquals("A", locus.refAllele());
      assertEquals(List.of("AT", "ATT"), locus.altAlleles());
      return locus.calls().stream()
          .collect(Collectors.toMap(SampleCall::sampleName, Function.identity()));
    }
  }

  @Test
  public void sampleCarryingAltOnSecondRecordRendersThatAltNotReference() {
    // S_REC2ONLY: hom-ref on record 1 (ALT=AT, non-contributing), hom-alt on
    // record 2 (ALT=ATT, contributing). Reading only record 1 - the original bug -
    // would render this sample as reference; it must render ATT instead. This is
    // the 21-of-43-samples case measured at the real Pf3D7_01_v3:538376 locus.
    SampleCall c = multiRecordCalls().get("S_REC2ONLY");
    assertFalse(c.noCall());
    assertEquals("ATT", c.allele());
    assertEquals(List.of("ATT", "ATT"), c.chromosomeAlleles());
    assertEquals("1/1", c.genotype());
    assertEquals(Integer.valueOf(8), c.depth());
    assertEquals("100.00", c.readFrequency());
  }

  @Test
  public void twoContributingRecordsSlashJoinDistinctAlleles() {
    // S_TWOREC carries an alt on BOTH record 1 (AT) and record 2 (ATT) - a true
    // multi-allelic site the pipeline split across records. Both must show, and
    // chromosomeAlleles (the aggregation weight) must carry both, not the reference.
    SampleCall c = multiRecordCalls().get("S_TWOREC");
    assertFalse(c.noCall());
    assertEquals("AT/ATT", c.allele());
    assertEquals(List.of("AT", "ATT"), c.chromosomeAlleles());
    // Slash-joined raw GTs of the contributing records - no longer a real VCF
    // genotype once merged, but not a lie either.
    assertEquals("0/1/0/1", c.genotype());
    // depth/readFrequency come from the FIRST contributing record (record 1).
    assertEquals(Integer.valueOf(10), c.depth());
    assertEquals("50.00", c.readFrequency());
  }

  @Test
  public void aminoAcidsUnionAcrossRecordsResolvedAgainstOwnCannIndex() {
    // Record 1's k0 and record 2's k0 are DIFFERENT CANN entries (L and P
    // respectively) despite sharing the same key string - proof that each is
    // resolved against its own record's CannIndex rather than a merged map, which
    // would either collide or silently pick the wrong one.
    SampleCall c = multiRecordCalls().get("S_TWOREC");
    assertEquals(List.of("L", "P"), c.aminoAcids());
  }

  @Test
  public void homRefOnEveryRecordStillReadsAsReferenceCall() {
    SampleCall c = multiRecordCalls().get("S_ALLHOMREF");
    assertFalse(c.noCall());
    assertEquals("A", c.allele());
    assertEquals(List.of("A", "A"), c.chromosomeAlleles());
    assertEquals("0/0", c.genotype());
  }

  @Test
  public void noCallOnEveryRecordStillReadsAsNoCall() {
    SampleCall c = multiRecordCalls().get("S_ALLNOCALL");
    assertTrue(c.noCall());
    assertEquals("", c.allele());
    assertNull(c.depth());
    assertNull(c.readFrequency());
    assertEquals(List.of(), c.chromosomeAlleles());
  }
}
