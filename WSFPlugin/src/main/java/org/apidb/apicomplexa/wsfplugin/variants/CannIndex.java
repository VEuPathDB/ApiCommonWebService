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

    Map<String, CannEntry> byKey = new LinkedHashMap<>();
    for (String raw : cannValue.split(",")) {
      if (raw.isEmpty() || ".".equals(raw)) continue;
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
