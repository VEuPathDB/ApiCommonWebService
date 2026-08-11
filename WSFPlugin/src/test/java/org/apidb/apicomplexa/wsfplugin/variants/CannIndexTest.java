package org.apidb.apicomplexa.wsfplugin.variants;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CannIndexTest {

  /** Real CANN from Pf3D7_01_v3:29514. */
  private static final String CANN =
      "r0|GTG|V|reference|PF3D7_0100100.1|5|2|.|.," +
      "r1|GTN|V|reference|PF3D7_0100100.1|5|2|.|.," +
      "k0|GCG|A|missense|PF3D7_0100100.1|5|2|c.5T>C|p.Val2Ala," +
      "k1|ATG|M|missense|PF3D7_0100100.1|5|2|.|p.Val2Met";

  @Test
  public void parsesEntriesByKey() {
    CannIndex index = CannIndex.parse(CANN);
    CannEntry k0 = index.get("k0").orElseThrow();
    assertEquals("GCG", k0.codon());
    assertEquals("A", k0.aminoAcid());
    assertEquals("missense", k0.effect());
    assertEquals("PF3D7_0100100.1", k0.transcriptId());
    assertEquals("c.5T>C", k0.hgvsC());
  }

  @Test
  public void resolvesSingleKey() {
    assertEquals(List.of("A"), CannIndex.parse(CANN).aminoAcidsFor("k0"));
  }

  @Test
  public void resolvesDiploidSlotsAndDeduplicates() {
    // Two slots, two keys, same amino acid V -> one value, not two.
    assertEquals(List.of("V"), CannIndex.parse(CANN).aminoAcidsFor("r0/r1"));
    // Distinct amino acids keep both, in encounter order.
    assertEquals(List.of("V", "A"), CannIndex.parse(CANN).aminoAcidsFor("r0/k0"));
  }

  @Test
  public void resolvesPhasedSeparator() {
    assertEquals(List.of("V", "A"), CannIndex.parse(CANN).aminoAcidsFor("r0|k0"));
  }

  @Test
  public void resolvesMultipleTranscriptKeysForOneAllele() {
    assertEquals(List.of("A", "M"), CannIndex.parse(CANN).aminoAcidsFor("k0;k1"));
  }

  @Test
  public void bareRefAndMissingYieldNothing() {
    CannIndex index = CannIndex.parse(CANN);
    assertTrue(index.aminoAcidsFor("r").isEmpty());
    assertTrue(index.aminoAcidsFor(".").isEmpty());
    assertTrue(index.aminoAcidsFor("").isEmpty());
    assertTrue(index.aminoAcidsFor(null).isEmpty());
  }

  @Test
  public void emptyCannParsesToEmptyIndex() {
    assertTrue(CannIndex.parse(".").aminoAcidsFor("k0").isEmpty());
    assertTrue(CannIndex.parse(null).aminoAcidsFor("k0").isEmpty());
  }

  @Test
  public void unknownKeyIsSkippedNotThrown() {
    assertEquals(List.of("A"), CannIndex.parse(CANN).aminoAcidsFor("k0/k99"));
  }

  @Test
  public void truncatedEntryIsDroppedWithoutAffectingItsNeighbour() {
    CannIndex index = CannIndex.parse("k0|GCG|A," + "k1|ATG|M|missense|T1.1|5|2|.|p.Val2Met");
    assertTrue("truncated entry dropped", index.get("k0").isEmpty());
    assertEquals("well-formed neighbour survives", "M", index.get("k1").orElseThrow().aminoAcid());
    assertEquals(List.of("M"), index.aminoAcidsFor("k0/k1"));
  }

  @Test
  public void dotAminoAcidContributesNothing() {
    CannIndex index = CannIndex.parse("k0|GCG|.|missense|PF3D7_0100100.1|5|2|.|.," + "k1|ATG|M|missense|PF3D7_0100100.1|5|2|.|p.Val2Met");
    assertTrue(index.aminoAcidsFor("k0").isEmpty());
    assertEquals(List.of("M"), index.aminoAcidsFor("k0/k1"));
  }
}
