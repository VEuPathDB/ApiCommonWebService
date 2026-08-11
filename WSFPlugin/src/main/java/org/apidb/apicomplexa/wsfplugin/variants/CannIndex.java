package org.apidb.apicomplexa.wsfplugin.variants;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Index over a locus's CANN entries, and the CA -> amino acid resolution that the
 * per-strain table needs.
 *
 * CA (a FORMAT field) names the CANN key(s) for one sample's genotype:
 *   - allele slots are separated by '/' (unphased) or '|' (phased)
 *   - multiple transcript keys for ONE allele are separated by ';'
 *   - a bare "r" means "reference allele, no CDS annotation"
 *   - "." means missing
 */
public class CannIndex {

  private static final CannIndex EMPTY = new CannIndex(Map.of());

  private final Map<String, CannEntry> _byKey;

  private CannIndex(Map<String, CannEntry> byKey) {
    _byKey = byKey;
  }

  public static CannIndex parse(String cannValue) {
    if (cannValue == null || cannValue.isEmpty() || ".".equals(cannValue)) return EMPTY;
    return parse(List.of(cannValue.split(",")));
  }

  /**
   * Parses CANN entries already split into one string per entry. This is the shape
   * htsjdk actually produces for this attribute: CANN is declared Number=. in the
   * VCF header, so htsjdk parses it as a List<String> rather than a String. Calling
   * vc.getAttributeAsString("CANN", null) on a List-valued attribute does NOT return
   * the raw value - it returns the List's toString(), e.g.
   * "[r0|TGT|C|reference|...|., k0|AGT|S|missense|...]" (square brackets, ", "
   * separators). Splitting THAT on ',' produces a first key of "[r0" and every
   * later key with a leading space, so no key ever matches a CA value and
   * aminoAcidsFor() silently returns empty for every sample (verified against the
   * real merged.ann.vcf.gz via HtsjdkRealFileSpikeTest#diagnoseCannAttributeShapeAtRealLocus).
   *
   * The caller (MergedVcfReader.parseCann) branches on the raw attribute's runtime
   * type and calls this overload when it is a List. parse(String) remains for
   * fixtures/tests and for the case where htsjdk hands back a bare String; it
   * delegates here so there is exactly one parsing implementation, not two that
   * can drift.
   */
  public static CannIndex parse(List<String> entries) {
    if (entries == null || entries.isEmpty()) return EMPTY;

    Map<String, CannEntry> byKey = new LinkedHashMap<>();
    for (String raw : entries) {
      if (raw == null || raw.isEmpty() || ".".equals(raw)) continue;
      // -1 keeps trailing empty fields, so a truncated entry is skipped rather than
      // silently shifting every field left.
      String[] f = raw.split("\\|", -1);
      if (f.length < 9) continue;
      byKey.put(f[0], new CannEntry(f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8]));
    }
    return byKey.isEmpty() ? EMPTY : new CannIndex(byKey);
  }

  public Optional<CannEntry> get(String key) {
    return Optional.ofNullable(_byKey.get(key));
  }

  /**
   * Distinct amino acids named by a sample's CA value, in encounter order.
   * A sample legitimately resolves to several when the locus sits in a
   * multi-transcript gene.
   */
  public List<String> aminoAcidsFor(String caValue) {
    List<String> out = new ArrayList<>();
    if (caValue == null || caValue.isEmpty() || ".".equals(caValue)) return out;

    for (String slot : caValue.split("[/|]")) {
      for (String key : slot.split(";")) {
        if (key.isEmpty() || ".".equals(key) || "r".equals(key)) continue;
        CannEntry entry = _byKey.get(key);
        if (entry == null) continue;
        String aa = entry.aminoAcid();
        if (aa == null || aa.isEmpty() || ".".equals(aa)) continue;
        if (!out.contains(aa)) out.add(aa);
      }
    }
    return out;
  }
}
