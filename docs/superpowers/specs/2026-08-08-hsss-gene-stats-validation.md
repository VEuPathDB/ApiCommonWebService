# Validation: corrected geneChars statistics

2026-08-08. Acceptance evidence for
`2026-08-07-hsss-gene-stats-fix-design.md`, run against the `plasmo.jbrestel` dev
instance with appDb `unidb_shu_a`.

## What was deployed

All three halves must be present or the search fails; this run had all three:

| piece | evidence |
|---|---|
| perl filter | `$GUS_HOME/bin/hsssGeneCharacteristicsFilter` md5 `dfee8785…` == checkout |
| plugin jar | `api-common-websvc-wsfplugin-1.0.0.jar` rebuilt via `bld ApiCommonWebService/WSFPlugin`, contains `span_snp_density` |
| model | `span_snp_density` in the `wsColumn` list and both `postCacheUpdateSql` blocks; webapp reloaded |

The jar is NOT rebuilt by `wb model`. `bld ApiCommonWebService/WSFPlugin` followed by an
`instance_manager … reload` is what deploys it.

## Run

`GenesByNgsSnps`, organism *Plasmodium falciparum 3D7*, `variation_sample_meta` left at
its default `{"filters":[]}` (all samples), every threshold permissive
(`snp_class=All SNVs`, occurrence/ratio/density bounds unset). 5,590 transcripts
returned.

## 1. dN/dS is exactly reproducible — 31/31, max error 0.000000

The design doc predicted only a *correlation* here, expecting divergence from different
classifiers. That was too pessimistic about the wrong thing: the classifier difference
affects the COUNTS, but this check tests the NORMALIZATION, and both sides draw site
counts from `apidbtuning.GeneVariationSummary`. So it is exact, not correlated.

For 31 sampled genes, comparing the search's reported `ngs_dn_ds_ratio` against
`(nonsyn/nonsyn_sites) / (syn/syn_sites)` computed in SQL from the tuning table, using
the search's own synonymous and missense counts:

```
 genes | dnds_exact | missing_sites | max_abs_err
    31 |         31 |             0 |    0.000000
```

## 2. CDS density is exact

`PF3D7_0100100`: 1,743 total variants, 99 unclassified, so 1,644 coding.
`cds_length` = 6,492. `1000 × 1644 / 6492 = 253.23`. Search reported **253.23**.

Note this gene also demonstrates the defect being fixed: its span density is 228.86
against a CDS density of 253.23. The old code reported the 228.86 figure under the label
"SNPs per Kb (CDS)".

## 3. The biology moved the right way

```
dN/dS over 4,688 transcripts with a defined value
  median          0.4718
  below 1         3,967  (85%)
  max             8.3019
  exactly zero      218
```

Median **0.4718**, with 85% under 1 — the expected purifying-selection signature.

Two independent corroborations:
- `GeneVariationSummary` computes piN/piS by a completely different route (SnpEff
  severity classes, frequency-weighted, Nei-Gojobori sites) and reports a pfal3D7 median
  of **0.512**. Two pipelines, different classifiers, converging.
- The tuning table's design notes record that WITHOUT site normalization the median
  piN/piS is **2.0**, implying genome-wide positive selection. That is the regime the old
  un-normalized count ratio was in.

The 218 genes at exactly 0 are the `defined($dn)` case: zero missense variants gives
dN = 0 and a real ratio of 0, the strongest purifying signal available. Truth-testing
`$dn` instead of `defined($dn)` would have silently converted all 218 into "no value".

## 4. Coverage regression: non-coding genes survive

254 returned transcripts have a blank CDS density. **All 254 have a populated span
density** — verified programmatically, not by inspection. Examples:

```
PF3D7_0100500   cds=(blank)  span=45.05   dnds=(blank)  total=5
PF3D7_0101400   cds=(blank)  span=18.14   dnds=(blank)  total=15
PF3D7_0101500   cds=(blank)  span=69.70   dnds=(blank)  total=115
```

Consistent with the appDb: 283 pfal3D7 genes have `cds_length IS NULL`; 254 of them have
at least one variant and so appear.

902 transcripts have no dN/dS — genes with no synonymous sites or no tuning-table row.
Both groups are returned because the corresponding filters were left unset; narrowing
either excludes them, which is the intended semantics.

## What this run does NOT establish

- **The filters were not exercised under narrowing.** Every bound was left permissive.
  The exclusion paths (`return 0` when a statistic is undefined and the filter has been
  narrowed) are covered by the unit fixture in `hsssTestSuite`, not here.
- **The counts themselves are unchanged and unverified by this work.** Only the two
  normalized statistics were touched. HSSS's classification of a position as synonymous,
  missense, nonsense or unclassified is exactly as before.
- **No comparison to pre-change output was made on this instance.** The old values are
  knowable from the formula (`total/span` and `nonsyn/syn`) but were not captured from a
  running pre-change instance.
