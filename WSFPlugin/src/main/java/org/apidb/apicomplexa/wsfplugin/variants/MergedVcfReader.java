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

  /**
   * The pipeline's write_vcf_entry emits one VCF record per unique ALT, so a locus
   * routinely has several records at the same CHROM/POS (measured against the real
   * merged.ann.vcf.gz: 9,384 of 47,189 positions, 19.9%, have more than one record;
   * one locus had seven). Reading only the first record - the original bug - drops
   * every sample whose alt lives on a later record: measured, 13.9% of sample-locus
   * alt calls involve a sample carrying alts on more than one record. This reads
   * EVERY record at the position and merges them into one LocusCalls; see
   * {@link #mergeSample} for the per-sample merge rules.
   */
  public Optional<LocusCalls> read(String sequenceId, int position) {
    Iterator<VariantContext> it = _reader.query(sequenceId, position, position);
    List<VariantContext> records = new ArrayList<>();
    while (it.hasNext()) records.add(it.next());
    if (records.isEmpty()) return Optional.empty();

    String ref = records.get(0).getReference().getBaseString();

    // Union of ALTs across records, in record order - each record's own ALT(s),
    // de-duplicated (the same ALT should not appear twice even though the pipeline
    // does not normally repeat one across records at a locus).
    List<String> alts = new ArrayList<>();
    List<CannIndex> cannByRecord = new ArrayList<>(records.size());
    for (VariantContext vc : records) {
      for (Allele a : vc.getAlternateAlleles()) {
        String base = a.getBaseString();
        if (!alts.contains(base)) alts.add(base);
      }
      // CANN keys (r0, k0, ...) are scoped to their own record - record 1's k0 and
      // record 2's k0 are different entries. Build one CannIndex per record and
      // NEVER merge them into a single map, or a sample's CA would resolve against
      // the wrong record's key and silently produce the wrong amino acid.
      cannByRecord.add(parseCann(vc));
    }

    List<SampleCall> calls = new ArrayList<>();
    for (String sample : _reader.getFileHeader().getGenotypeSamples()) {
      calls.add(mergeSample(records, cannByRecord, sample));
    }
    return Optional.of(new LocusCalls(sequenceId, position, ref, alts, calls));
  }

  /**
   * Merges one sample's genotype across every record at the locus.
   *
   * - noCall: true only if the genotype is no-call (or absent) on every record.
   * - a "contributing" record is one where the sample's genotype is present and
   *   NOT hom-ref - a hom-ref call on a record whose alt the sample doesn't carry
   *   is the pipeline's per-ALT splitting, not evidence of a reference call, and
   *   must not drag the merged result toward reference.
   * - chromosomeAlleles (the aggregation weight):
   *     no contributing records, but at least one real call -> reference call:
   *       the genotype alleles from the first record with a real call.
   *     exactly one contributing record -> that record's genotype alleles as-is.
   *     more than one contributing record -> the non-reference allele from each
   *       contributing record (a true multi-allelic site the pipeline split).
   * - aminoAcids: the union across ALL records where the sample's CA resolves,
   *   each against its OWN record's CannIndex, de-duplicated, in record order
   *   then within-record order.
   * - depth: from a contributing record if there is one, else the first record
   *   with a real call.
   * - readFrequency: from the record whose alt the sample carries; with multiple
   *   contributing records, the first.
   * - coverageFilled: only if there is no contributing record AND the reference
   *   call is coverage-filled by the existing rule (hom-ref, no RO/AO).
   * - genotype: the raw GT when exactly one record contributed (or it is a plain
   *   reference/no-call); otherwise the slash-joined raw GTs of the contributing
   *   records. No longer shown on the page, but still goes to downloads.
   * - ploidy: the max ploidy observed across records where the sample has a real
   *   call.
   */
  private SampleCall mergeSample(List<VariantContext> records, List<CannIndex> cannByRecord,
                                 String sample) {
    int n = records.size();
    List<Genotype> genotypes = new ArrayList<>(n);
    for (VariantContext vc : records) genotypes.add(vc.getGenotype(sample));

    List<Integer> realCallIdx = new ArrayList<>();
    List<Integer> contributingIdx = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      Genotype g = genotypes.get(i);
      if (g == null || g.isNoCall()) continue;
      realCallIdx.add(i);
      if (!g.isHomRef()) contributingIdx.add(i);
    }

    if (realCallIdx.isEmpty()) {
      return new SampleCall(sample, ".", null, "", null, List.of(), List.of(), 0, true, false);
    }

    int ploidy = 0;
    for (int i : realCallIdx) ploidy = Math.max(ploidy, genotypes.get(i).getPloidy());

    List<String> aminoAcids = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      Genotype g = genotypes.get(i);
      if (g == null) continue;
      for (String aa : cannByRecord.get(i).aminoAcidsFor(asString(g.getExtendedAttribute("CA")))) {
        if (!aminoAcids.contains(aa)) aminoAcids.add(aa);
      }
    }

    List<String> chromosomeAlleles;
    String genotypeStr;
    int depthFreqRecord;

    if (contributingIdx.isEmpty()) {
      int firstReal = realCallIdx.get(0);
      chromosomeAlleles = chromosomeAlleles(genotypes.get(firstReal));
      genotypeStr = genotypeString(records.get(firstReal), genotypes.get(firstReal));
      depthFreqRecord = firstReal;
    } else if (contributingIdx.size() == 1) {
      int only = contributingIdx.get(0);
      chromosomeAlleles = chromosomeAlleles(genotypes.get(only));
      genotypeStr = genotypeString(records.get(only), genotypes.get(only));
      depthFreqRecord = only;
    } else {
      chromosomeAlleles = new ArrayList<>();
      List<String> rawGts = new ArrayList<>();
      for (int i : contributingIdx) {
        chromosomeAlleles.add(firstNonRefAllele(genotypes.get(i)));
        rawGts.add(genotypeString(records.get(i), genotypes.get(i)));
      }
      genotypeStr = String.join("/", rawGts);
      depthFreqRecord = contributingIdx.get(0);
    }

    VariantContext depthFreqVc = records.get(depthFreqRecord);
    Genotype depthFreqG = genotypes.get(depthFreqRecord);

    Integer depth = depthFreqG.hasDP() && depthFreqG.getDP() >= 0
        ? Integer.valueOf(depthFreqG.getDP()) : null;

    // A coverage-filled reference call carries GT and DP only; every other FORMAT
    // field was written as '.' by fill_missing_coverage_gt. Only possible when
    // nothing contributed - a contributing record's own alt was actually called.
    int[] ro = intsOf(depthFreqG.getExtendedAttribute("RO"));
    int[] ao = intsOf(depthFreqG.getExtendedAttribute("AO"));
    boolean filled = contributingIdx.isEmpty() && depthFreqG.isHomRef()
        && ro.length == 0 && ao.length == 0;

    return new SampleCall(
        sample,
        genotypeStr,
        depth,
        allele(chromosomeAlleles),
        readFrequency(depthFreqVc, depthFreqG, ro, ao, filled),
        aminoAcids,
        chromosomeAlleles,
        ploidy,
        false,
        filled);
  }

  /** The genotype's non-reference allele, for a record already known to be contributing. */
  private String firstNonRefAllele(Genotype g) {
    for (Allele a : g.getAlleles()) {
      if (!a.isReference() && !a.isNoCall()) return a.getBaseString();
    }
    // Contributing means "not hom-ref", so this should be unreachable; fall back
    // defensively rather than throwing.
    return g.getAlleles().isEmpty() ? "" : g.getAlleles().get(0).getBaseString();
  }

  /**
   * CANN is declared Number=. in the VCF header, so htsjdk parses it as a
   * List<String> (one element per comma-separated entry), NOT as a bare String -
   * verified against the real merged.ann.vcf.gz (see
   * HtsjdkRealFileSpikeTest#diagnoseCannAttributeShapeAtRealLocus): CANN's raw
   * attribute class there is java.util.ArrayList.
   *
   * Calling vc.getAttributeAsString("CANN", null) on that List-valued attribute
   * does NOT return the raw value - it returns the List's toString(): bracketed,
   * ", "-separated, e.g. "[r0|TGT|C|reference|...|., k0|AGT|S|missense|...]".
   * Splitting THAT on ',' produces a first key of "[r0" and every later key with a
   * leading space, so no key ever matches a CA value and aminoAcidsFor() silently
   * returns empty for every sample. That was the bug.
   *
   * htsjdk's getAttributeAsStringList(key, default) is NOT the fix by itself: for
   * a genuine List attribute it returns the list's elements unchanged (correct -
   * one already-split entry per element), but for a String-valued attribute it
   * wraps the whole string as a SINGLETON list (CommonInfo.getAttributeAsList,
   * checked via javap) - it does not split on ',' the way CannIndex.parse(String)
   * does. So blindly calling it on a String attribute would hand CannIndex one
   * "entry" that is actually several comma-joined entries glued together, corrupting
   * the '|'-split fields at the join. Branch on the raw attribute's runtime type
   * instead of assuming either shape.
   */
  private CannIndex parseCann(VariantContext vc) {
    Object raw = vc.getAttribute("CANN");
    if (raw instanceof List) {
      // Elements come from htsjdk's own VCF value parsing and are Strings in
      // practice, but the field is untyped (List<?> at the getAttribute() level) -
      // stringify defensively rather than casting straight to List<String>.
      List<String> entries = new ArrayList<>();
      for (Object o : (List<?>) raw) entries.add(o == null ? null : String.valueOf(o));
      return CannIndex.parse(entries);
    }
    return CannIndex.parse(vc.getAttributeAsString("CANN", null));
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

  /**
   * Display allele, derived from the sample's distinct chromosome alleles (see
   * {@link #chromosomeAlleles}, or the merged multi-record equivalent built in
   * {@link #mergeSample}):
   *   - 1 distinct -> that allele
   *   - 2 distinct, both single-character -> IUPAC ambiguity code
   *   - otherwise -> slash-joined distinct alleles, e.g. "ATT/ATATT"
   * This unifies the pre-existing within-record het case (a diploid SNP het) with
   * the new across-record case (a locus the pipeline split into several records,
   * where a sample carries different alts on different records).
   */
  private String allele(List<String> chromosomeAlleles) {
    List<String> distinct = new ArrayList<>();
    for (String s : chromosomeAlleles) if (!distinct.contains(s)) distinct.add(s);

    if (distinct.isEmpty()) return "";
    if (distinct.size() == 1) return distinct.get(0);

    if (distinct.size() == 2 && distinct.get(0).length() == 1 && distinct.get(1).length() == 1) {
      String key = distinct.get(0).compareTo(distinct.get(1)) < 0
          ? distinct.get(0) + distinct.get(1)
          : distinct.get(1) + distinct.get(0);
      String iupac = IUPAC.get(key);
      if (iupac != null) return iupac;
    }
    return String.join("/", distinct);
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
