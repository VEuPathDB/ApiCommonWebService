package org.apidb.apicomplexa.wsfplugin.variants;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Random access into the dnaseq pipeline's merged annotated VCF, by (sequence, position).
 *
 * Deliberately free of WDK types so it can be unit-tested against a fixture with no
 * database and no container, and so a future REST-backed consumer can use it directly.
 */
public class MergedVcfReader implements AutoCloseable {

  private static final Map<String, String> IUPAC = Map.of(
      "AC", "M", "AG", "R", "AT", "W", "CG", "S", "CT", "Y", "GT", "K");

  /** intsOf sentinel for a '.' (missing) slot — a read count is never negative. */
  private static final int MISSING = -1;

  private final VCFFileReader _reader;

  public MergedVcfReader(Path vcfPath) {
    // requireIndex = true: streaming an 809MB file per page view is not a fallback.
    _reader = new VCFFileReader(new File(vcfPath.toString()), true);
  }

  public Optional<LocusCalls> read(String sequenceId, int position) {
    Iterator<VariantContext> it = _reader.query(sequenceId, position, position);
    if (!it.hasNext()) return Optional.empty();

    VariantContext vc = it.next();
    String ref = vc.getReference().getBaseString();
    List<String> alts = new ArrayList<>();
    for (Allele a : vc.getAlternateAlleles()) alts.add(a.getBaseString());

    CannIndex cann = CannIndex.parse(vc.getAttributeAsString("CANN", null));

    List<SampleCall> calls = new ArrayList<>();
    for (String sample : _reader.getFileHeader().getGenotypeSamples()) {
      calls.add(toCall(vc, vc.getGenotype(sample), sample, ref, cann));
    }
    return Optional.of(new LocusCalls(sequenceId, position, ref, alts, cann, calls));
  }

  private SampleCall toCall(VariantContext vc, Genotype g, String sample, String ref,
                            CannIndex cann) {
    // Null check FIRST: a no-call genotype must not be dereferenced below.
    if (g == null || g.isNoCall()) {
      return new SampleCall(sample, ".", null, "", null, List.of(), List.of(), 0, true, false);
    }

    Integer depth = g.hasDP() && g.getDP() >= 0 ? Integer.valueOf(g.getDP()) : null;

    // A coverage-filled reference call carries GT and DP only; every other FORMAT
    // field was written as '.' by fill_missing_coverage_gt. It is distinguishable
    // from a real reference call, which carries real RO/AO.
    int[] ro = intsOf(g.getExtendedAttribute("RO"));
    int[] ao = intsOf(g.getExtendedAttribute("AO"));
    boolean filled = g.isHomRef() && ro.length == 0 && ao.length == 0;

    return new SampleCall(
        sample,
        genotypeString(vc, g),
        depth,
        allele(g, ref),
        readFrequency(vc, g, ro, ao, filled),
        cann.aminoAcidsFor(asString(g.getExtendedAttribute("CA"))),
        chromosomeAlleles(g),
        g.getPloidy(),
        false,
        filled);
  }

  /** One entry per chromosome slot, duplicates kept — this is the aggregation weight. */
  private List<String> chromosomeAlleles(Genotype g) {
    List<String> out = new ArrayList<>();
    for (Allele a : g.getAlleles()) {
      if (a.isNoCall()) continue;
      out.add(a.getBaseString());
    }
    return out;
  }

  /** IUPAC for a het of two single-base alleles; otherwise the first non-ref allele. */
  private String allele(Genotype g, String ref) {
    List<String> bases = new ArrayList<>();
    for (Allele a : g.getAlleles()) {
      if (a.isNoCall()) continue;
      String s = a.getBaseString();
      if (!bases.contains(s)) bases.add(s);
    }
    if (bases.isEmpty()) return "";
    if (bases.size() == 1) return bases.get(0);

    if (bases.size() == 2 && bases.get(0).length() == 1 && bases.get(1).length() == 1) {
      String key = bases.get(0).compareTo(bases.get(1)) < 0
          ? bases.get(0) + bases.get(1)
          : bases.get(1) + bases.get(0);
      String iupac = IUPAC.get(key);
      if (iupac != null) return iupac;
    }
    for (String b : bases) if (!b.equals(ref)) return b;
    return bases.get(0);
  }

  private String readFrequency(VariantContext vc, Genotype g, int[] ro, int[] ao, boolean filled) {
    if (filled) return "100.00";
    if (ro.length == 0 && ao.length == 0) return null;

    int refCount = ro.length > 0 ? ro[0] : 0;

    if (g.isHomRef()) {
      // Reference support is against ALL non-reference reads, not just one alt.
      // A sentinel slot (that alt's AO wasn't reported) contributes nothing.
      int altTotal = sumReal(ao);
      int total = refCount + altTotal;
      if (total <= 0) return null;
      return String.format(Locale.ROOT, "%.2f", refCount * 100.0 / total);
    }

    // Alt call: support for the allele this sample actually carries, found by its
    // VCF-wide allele index (1-based over alts; 0 is ref), not the sum of every AO
    // entry. The published file is biallelic by construction — write_vcf_entry
    // splits multi-allelic sites into one record per alt (measured: 40,000 loci,
    // 548,040 alt calls, zero multi-alt records, zero samples with >1 nonzero AO)
    // — so this indexing is defensive rather than hot; only the synthetic
    // multi-alt fixture records below exercise it.
    int altSupport = MISSING;
    for (Allele a : g.getAlleles()) {
      if (a.isReference() || a.isNoCall()) continue;
      int idx = vc.getAlleleIndex(a) - 1;
      if (idx >= 0 && idx < ao.length && ao[idx] != MISSING) {
        altSupport = ao[idx];
        break;
      }
    }
    if (altSupport == MISSING) {
      // Index out of range, or the AO array holds a sentinel at that slot (this
      // sample's specific alt has no reported AO even though the field carries
      // other alts' values) - fall back to the summed real AO rather than
      // throwing or reporting a bogus neighbour's count.
      altSupport = sumReal(ao);
    }
    int total = refCount + altSupport;
    if (total <= 0) return null;
    return String.format(Locale.ROOT, "%.2f", altSupport * 100.0 / total);
  }

  /** Sums only the real (non-sentinel) entries of an intsOf() result. */
  private static int sumReal(int[] vals) {
    int sum = 0;
    for (int v : vals) if (v != MISSING) sum += v;
    return sum;
  }

  /**
   * The raw VCF genotype, e.g. "0", "0/1", "1|1". Indices come from the VariantContext,
   * which is the only thing that knows the locus's full allele ordering — deriving them
   * from the genotype's own alleles would misnumber a sample that carries only alt 2.
   */
  private String genotypeString(VariantContext vc, Genotype g) {
    StringBuilder sb = new StringBuilder();
    List<Allele> alleles = g.getAlleles();
    for (int i = 0; i < alleles.size(); i++) {
      if (i > 0) sb.append(g.isPhased() ? '|' : '/');
      Allele a = alleles.get(i);
      sb.append(a.isNoCall() ? "." : String.valueOf(vc.getAlleleIndex(a)));
    }
    return sb.toString();
  }

  private static String asString(Object o) {
    return o == null ? null : String.valueOf(o);
  }

  /**
   * Parses a comma-separated integer FORMAT value, e.g. AO="1,.,4". A '.' or empty
   * slot becomes the {@link #MISSING} sentinel rather than being dropped — dropping
   * it would shift every later slot left and misalign an index-based lookup (a
   * sample called for alt2 would silently read alt3's count).
   *
   * A field that is entirely missing — absent, "", a bare "." with no commas, or
   * (deliberately, by the same rule) something like ".,." with every slot blank —
   * collapses to an empty array. That is the "no data at all" case: it must equal
   * what a completely absent field produces, because {@code readFrequency}'s
   * coverage-filled check (and the null-vs-100.00 decision) tests {@code
   * length == 0} for "this FORMAT field carries nothing", not "every slot happens
   * to be a sentinel". A field with at least one real value keeps its full,
   * positionally-aligned length, sentinels included.
   */
  private static int[] intsOf(Object raw) {
    String s = asString(raw);
    if (s == null || s.isEmpty() || ".".equals(s)) return new int[0];
    String[] parts = s.split(",");
    int[] out = new int[parts.length];
    boolean anyReal = false;
    for (int i = 0; i < parts.length; i++) {
      String p = parts[i].trim();
      if (p.isEmpty() || ".".equals(p)) {
        out[i] = MISSING;
        continue;
      }
      try {
        out[i] = Integer.parseInt(p);
        anyReal = true;
      } catch (NumberFormatException ignored) {
        out[i] = MISSING;
      }
    }
    return anyReal ? out : new int[0];
  }

  @Override
  public void close() {
    _reader.close();
  }
}
