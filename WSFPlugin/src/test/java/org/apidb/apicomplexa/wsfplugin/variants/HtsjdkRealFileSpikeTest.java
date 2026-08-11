package org.apidb.apicomplexa.wsfplugin.variants;

import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeType;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Spike against the real published VCF. Skipped unless -Dvariants.vcf is supplied,
 * because the file is 809MB and lives only on the webserver.
 *
 * Run:
 *   mvn -pl WSFPlugin test -Dtest=HtsjdkRealFileSpikeTest \
 *     -Dvariants.vcf=/var/www/Common/apiSiteFilesMirror/webServices/PlasmoDB/build-71/Pfalciparum3D7/dnaseq/vcf/merged.ann.vcf.gz
 */
public class HtsjdkRealFileSpikeTest {

  @Test
  public void readsRealFileDespiteUnderDeclaredHeader() {
    String path = System.getProperty("variants.vcf");
    Assume.assumeNotNull(path);

    // requireIndex=true below: a .tbi must sit beside the file, or this throws
    // rather than skipping - that's an index problem, not an htsjdk problem.
    try (VCFFileReader reader = new VCFFileReader(new File(path), true)) {
      assertNotNull("header", reader.getFileHeader());
      assertFalse("samples", reader.getFileHeader().getGenotypeSamples().isEmpty());

      // Pf3D7_01_v3:29514 is a coding locus with a populated CANN (verified).
      Iterator<VariantContext> it = reader.query("Pf3D7_01_v3", 29514, 29514);
      assertTrue("locus found", it.hasNext());
      VariantContext vc = it.next();

      assertNotNull("CANN", vc.getAttributeAsString("CANN", null));

      Genotype g = vc.getGenotype(0);
      // getType() never returns null even on a degraded parse (it falls back to
      // NO_CALL/UNAVAILABLE), so assert the specific independently-verified call
      // rather than merely non-null - sample 0 here is a verified hom-ref (0:178:...).
      assertEquals("GT should be the independently-verified hom-ref call",
          GenotypeType.HOM_REF, g.getType());
      assertTrue("DP present", g.hasDP());
      assertNotNull("CA", g.getExtendedAttribute("CA"));
    }
  }

  /**
   * Diagnostic for the aa_product-always-empty bug: proves (or disproves) that
   * CANN, declared Number=. in the header, is parsed by htsjdk as a List<String>
   * rather than a String, so getAttributeAsString() round-trips it through the
   * list's toString() (bracketed, ", "-separated) instead of handing back the
   * raw comma-separated value CannIndex.parse(String) expects.
   *
   * Ground truth at Pf3D7_01_v3:100057: sample 5.1 has CA=r0, and CANN's r0 entry
   * has amino acid C - so a correct parse must resolve "r0" to "C".
   */
  @Test
  public void diagnoseCannAttributeShapeAtRealLocus() {
    String path = System.getProperty("variants.vcf");
    Assume.assumeNotNull(path);

    try (VCFFileReader reader = new VCFFileReader(new File(path), true)) {
      Iterator<VariantContext> it = reader.query("Pf3D7_01_v3", 100057, 100057);
      assertTrue("locus found", it.hasNext());
      VariantContext vc = it.next();

      Object rawAttribute = vc.getAttribute("CANN");
      System.out.println("CANN raw attribute class: " + rawAttribute.getClass().getName());

      String asString = vc.getAttributeAsString("CANN", null);
      System.out.println("getAttributeAsString(CANN) (first 200 chars): "
          + asString.substring(0, Math.min(200, asString.length())));

      java.util.List<String> asList = vc.getAttributeAsStringList("CANN", "");
      System.out.println("getAttributeAsStringList(CANN) size: " + asList.size());
      System.out.println("getAttributeAsStringList(CANN) first element: "
          + (asList.isEmpty() ? "<empty>" : asList.get(0)));

      Object ca = vc.getGenotype("5.1").getExtendedAttribute("CA");
      System.out.println("sample 5.1 CA: " + String.valueOf(ca));

      CannIndex viaString = CannIndex.parse(asString);
      System.out.println("CannIndex.parse(getAttributeAsString(...)).aminoAcidsFor(\"r0\") = "
          + viaString.aminoAcidsFor("r0"));
    }
  }
}
