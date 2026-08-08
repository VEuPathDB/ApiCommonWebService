# Fixing the two broken statistics in the HSSS gene characteristics search

Design, 2026-08-07.

Subject: `GenesByNgsSnps` — the WDK search displayed as **SNV Characteristics Within a
Group of Samples**, backed by `FindGenesWithSnpCharsPlugin` and
`bin/hsssGeneCharacteristicsFilter`.

## 1. The problem

Two of the statistics this search reports do not compute what their labels claim, and a
third label describes a category it does not hold.

### 1.1 "SNPs per Kb (CDS)" is neither CDS nor coding

`hsssGeneCharacteristicsFilter`, `processGene`:

```perl
my $density = $snpsCount / (($filterEnd - $filterStart) / 1000);
```

`$snpsCount` is every variant position in the gene, coding or not. `$filterStart` and
`$filterEnd` come from `geneLocations.txt`, which the plugin builds from
`webready.GeneAttributes_p.start_min` / `end_max` — the **genomic span**, introns and
UTRs included. So the reported number is total variants per kb of genomic length. The
column label, the param prompts (`SNPs per KB (CDS) >=`), and the param help ("density
of coding snps ... / KB of coding sequence") are each wrong in both the numerator and
the denominator.

### 1.2 The nonsyn/syn ratio has no site normalization

```perl
my $dnds = $synCount ? $nonSynCount / $synCount : undef;
```

A raw count ratio. It carries the codon-bias distortion that the search's own PlasmoDB
description apologises for in prose:

> Due to the extreme codon bias in the *P. falciparum* genome, the ratio of
> non-synonymous to synonymous SNPs within each gene is much higher than expected. ...
> We are intending to calculate more reliable normalized Dn/Ds or Ka/Ks ratios in
> subsequent releases of PlasmoDB.

The magnitude is known and measured. `GeneVariationSummary` derives Nei-Gojobori site
counts inline from the genetic code and found the pooled synonymous-site fraction in
pfal3D7 to be **17.49%, not the textbook ~25%** — a 1.43x correction on every gene.
Without it the median piN/piS is 2.0, implying genome-wide positive selection; with it
the median is 0.512, the expected purifying-selection signature. An unnormalized count
ratio inherits the whole of that error.

### 1.3 "Non-coding SNPs" means "unclassified"

`hsssFindPolymorphic.c`:

```c
if (product == 'X') product = -1;   // X is an unknown product.  ignore these.
...
int productClass = 0;                        // noncoding
if (nonSyn) productClass = 2;
else if (refProduct > 0) productClass = 1;   // syn
if (nonsense) productClass *= -1;
```

Class `0` means no protein product byte was available. That covers positions genuinely
outside coding sequence **and** positions where the product could not be called. The
filter then derives `$nonCodingCount = $snpsCount - $codingCount`, so the column labelled
"Non-coding SNPs" is really "not classified as coding".

### 1.4 Related, already fixed separately

The `snp_class` enum offered `Non-Coding`, but `FindGenesWithSnpCharsPlugin.legalParams`
omitted `"noncoding"` (and listed `"coding"` twice), so selecting it threw
`PluginUserException` before the script ran, even though
`hsssGeneCharacteristicsFilter` branches on it. Fixed on this branch in a prior commit;
recorded here because it is the same class of defect — a display layer promising
something the compute layer does not deliver.

## 2. What is deliberately NOT changing

**Stop-gained stays out of the nonsynonymous count.** `$nonsenseCount++ if $productClass
< 0` catches classes `-1` and `-2`, and those loci are therefore absent from
`$nonSynCount`. A stop-gained change is an amino-acid-changing change, so `dN` understates
selection on genes carrying premature stops. Leaving it alone keeps the column semantics
exactly as they are today and keeps results comparable to historical ones. Revisit only
with a deliberate decision; do not "tidy" it.

**The statistic remains a count ratio, not piN/piS.** HSSS carries a per-locus minor
allele frequency (`nonMajorAllelesPct`, the fourth field of its per-SNP stream), so a
frequency-weighted piN/piS is reachable. It is out of scope here: Nei's `n/(n-1)`
correction needs an allele count, and HSSS carries `knownsPercent` x strain count without
ploidy. The consequence to communicate is that this ratio weights a singleton the same as
a 50%-frequency variant, and so will NOT equal the piN/piS reported by the
`GenesByVariantCharacteristics` search or the gene record page.

**The search is not being deprecated.** Per-sample-set analysis is a real workflow. The
whole-cohort `GenesByVariantCharacteristics` search covers a different question and does
not replace this one.

## 3. Design

### 3.1 The split that makes this tractable

Numerators are sample-set-dependent; denominators are not. HSSS keeps computing counts
over the selected samples. The normalizers — CDS length and Nei-Gojobori site counts —
are properties of the gene and the genetic code, already derived once in
`apidbtuning.GeneVariationSummary`. They meet in `geneLocations.txt`.

Reusing those columns rather than recomputing them is the point: both searches then rest
on one definition of a synonymous site, and there is no second place for it to drift.

### 3.2 Data flow

```
FindGenesWithSnpCharsPlugin.initForBashScript
  SELECT g.sequence_id, g.start_min, g.end_max, g.source_id,
         gvs.cds_length, gvs.syn_sites, gvs.nonsyn_sites
  FROM webready.GeneAttributes_p g
  LEFT JOIN apidbtuning.GeneVariationSummary gvs
         ON gvs.gene_source_id = g.source_id
        AND gvs.project_id     = g.project_id
  WHERE g.source_id IS NOT NULL
    AND g.organism = '<vocabulary-supplied organism>'
        |
        v
geneLocations.txt
  seq \t start \t end \t geneId \t cdsLen \t synSites \t nonsynSites
        |   sort keys are still -k 1,1 -k 2,2n; the new columns are appended, so
        |   apiSortNoLocale is unaffected
        v
hsssGeneCharacteristicsFilter
```

`LEFT JOIN`, never inner. A gene with no `GeneVariationSummary` row still gets its
locations line and still reports counts; only its normalized statistics are blank. This
matters: the tuning table holds one row per gene **that has cohort variants**, which in
unidb_shu_a is 5,579 of 5,720 annotated pfal3D7 genes.

### 3.3 What the filter computes

```perl
my ($filterContigId, $filterStart, $filterEnd, $filterGeneId,
    $filterCdsLen, $filterSynSites, $filterNonsynSites) = split(/\t/, $geneLocationLine);
...
my $cdsDensity  = $filterCdsLen ? 1000 * $codingCount / $filterCdsLen : undef;
my $spanDensity = 1000 * $snpsCount / ($filterEnd - $filterStart);
my $dn          = $filterNonsynSites ? $nonSynCount / $filterNonsynSites : undef;
my $ds          = $filterSynSites    ? $synCount    / $filterSynSites    : undef;
my $dnds        = (defined($dn) && $ds) ? $dn / $ds                      : undef;
```

`defined($dn)`, not `$dn`. A gene with zero nonsynonymous variants has `dN = 0` and a
genuine ratio of 0 — a strong purifying-selection signal, and exactly the kind of gene
someone searching a low dN/dS range wants. Testing `$dn` for truth would silently
convert that into "no value" and drop the gene. `$ds` is tested for truth on purpose:
zero there is a division by zero, not a result.

`$cdsDensity` uses `$codingCount` (`productClass != 0`), so numerator and denominator are
both about coding sequence. `$spanDensity` keeps today's definition under a name that
admits what it is.

Undefined denominators are expected, not exceptional: `syn_sites` and `cds_length` are
NULL for non-coding genes — 283 of 5,579 pfal3D7 rows, 2,837 of 11,689 tbruTREU927 rows,
196 of 10,029 afumAf293 rows.

### 3.4 Filter semantics for undefined values

The existing `-1`-means-no-upper-bound sentinel already gates each filter:

```perl
if ($densityMin || $densityMax != -1) { ... }
if ($dndsMin    || $dndsMax    != -1) { ... }
```

An untouched filter therefore applies no constraint, and a gene with an undefined
statistic is dropped only when the user actually filters on it. That is the desired
behaviour and it needs no new machinery — worth noting because the sibling
`GenesByVariantCharacteristics` search needed a `filterParam` to get the same property,
`numberRangeParam` having no way to express "untouched".

When the user HAS narrowed a range and the gene's value is undefined, the gene is
excluded. An unknown value cannot be shown to be in range.

The existing zero-denominator branch keeps its behaviour:

```perl
if ($synCount == 0 && $nonSynCount != 0) { return 0 unless $dndsMax == -1; }
```

### 3.5 Wire format

Output goes from 8 fields to 9, gaining span density:

```
geneId, cdsDensity, spanDensity, dnds, synCount, nonSynCount, nonCodingCount, nonsenseCount, snpsCount
```

`FindGenesWithSnpCharsPlugin.makeResultRow` hard-asserts `parts.length != 8` and
allocates `new String[11]`; both move. `getColumns` and the `wsColumn` list in
`geneQueries.xml` gain the new column.

**Internal column names do not change.** `cds_snp_density` becomes actually-CDS and
`ngs_dn_ds_ratio` becomes site-normalized, so both names become correct without a rename
that would break `attributesList summary=` and saved strategies. The new column is
`span_snp_density`.

### 3.6 Compatibility

A saved strategy filtering on density or on the ratio will return a **different result
set** after deploy. That is the fix working, not a regression, but it is user-visible and
needs a release note. The values change for every gene, not only edge cases: density
changes by the intron fraction of each gene, and the ratio by the 1.43x site-fraction
correction in pfal.

## 4. Honest labels

Part of the fix, not an alternative to it. The calculations become correct; the labels
must then describe what is correct, including where the search still differs from its
whole-cohort sibling.

### 4.1 Result columns (`geneQuestions.xml`)

| internal | current label | new label | help |
|---|---|---|---|
| `cds_snp_density` | SNPs per Kb (CDS) | **SNVs per kb (CDS)** | Coding variants per kilobase of coding sequence, using the representative transcript's CDS length. Blank for genes with no coding sequence. |
| `span_snp_density` | *(new)* | **SNVs per kb (gene span)** | All variants in the gene divided by its genomic length, introns and UTRs included. |
| `ngs_dn_ds_ratio` | Nonsyn/syn SNP ratio | **dN/dS (site-normalized)** | Nonsynonymous and synonymous counts each divided by the number of sites of that class, from Nei-Gojobori counts over the representative transcript. Below 1 suggests purifying selection. Stop-gained variants are counted as nonsense, not nonsynonymous, so they do not enter the numerator. This is a count ratio: it weights a rare variant the same as a common one, so it will not equal the piN/piS in the SNV Characteristics search. |
| `ngs_num_non_synonymous` | Nonsynonymous SNPs | **Missense SNVs** | Amino-acid-changing, excluding those that introduce a stop. |
| `num_nonsense` | Nonsense SNPs | **Stop-gained SNVs** | *(unchanged meaning)* |
| `num_noncoding` | Non-coding SNPs | **Unclassified SNVs** | No protein product could be assigned: positions outside coding sequence, and positions where the reference product was unavailable. |
| `ngs_total_snps` | Total SNPs | **SNVs in gene span** | Variant positions anywhere between the gene's start and end. |
| `ngs_num_synonymous` | Synonymous SNPs | **Synonymous SNVs** | *(unchanged meaning)* |

### 4.2 Params (`geneParams.xml`)

- `snp_class` prompt `SNP Class` -> **SNV Class**. Enum terms follow the columns:
  `Non-Coding` -> **Unclassified**, `Non-Synonymous` -> **Missense**,
  `Nonsense` -> **Stop-gained**. Internal values are UNCHANGED — they are a contract with
  `legalParams` and with the filter's branches.
- `occurrences_lower` / `_upper`: `Number of SNPs of above class` -> **Number of SNVs of
  the selected class**. The count wording is already correct; it is
  `HsssGeneCharsFilterScriptGenerator.pm`'s usage text that wrongly says "percent", and
  that is corrected too.
- `dn_ds_ratio_lower` / `_upper`: `Non-synonymous / synonymous SNP ratio` -> **dN/dS
  (site-normalized)**, help gaining the stop-gained and count-ratio caveats.
- `snp_density_lower` / `_upper`: `SNPs per KB (CDS)` -> **SNVs per kb (CDS)**; help
  loses "density of coding snps ... / KB of coding sequence" and states the CDS-length
  denominator and the blank-for-non-coding behaviour. This pair filters CDS density only;
  span density is reported but not filterable, to avoid a fifteenth and sixteenth param
  on a form that already carries fourteen.
- Every "leaving this parameter value empty means you don't care about the upper bound"
  stays. That sentinel is real behaviour and is what makes undefined values safe.

### 4.3 Question description

The PlasmoDB bullet promising normalized ratios "in subsequent releases" becomes a
statement that they are here, plus a pointer to `GenesByVariantCharacteristics` for the
frequency-weighted piN/piS over the whole cohort. One bullet added: these statistics are
computed over the samples you select and are not comparable to the precomputed ones.

## 5. Verification

**Unit.** `hsssTestSuite` already exercises a `"unit test"` path where
`FindGenesWithSnpCharsPlugin.initForBashScript` writes hardcoded gene filters rather than
querying. Extend those four rows with known site counts and assert exact expected values,
covering:
- a normal coding gene: both densities and the ratio
- `cds_length` NULL: CDS density blank, span density populated
- `syn_sites` NULL: ratio blank, both densities populated
- `synCount == 0` with `nonSynCount > 0`: existing exclusion branch still fires

**Whole-cohort cross-check.** Run the search with every sample selected, then compare
against SQL over `GeneVariationSummary`:

```sql
SELECT gene_source_id,
       (n_missense::numeric / nullif(nonsyn_sites,0))
     / nullif(n_synonymous::numeric / nullif(syn_sites,0), 0) AS dnds_from_gvs
FROM apidbtuning.GeneVariationSummary
WHERE org_abbrev = 'pfal3D7';
```

These will correlate strongly but WILL NOT match, for reasons that are correct:
- HSSS classifies from reference product bytes; `GeneVariationSummary` uses SnpEff
  severity. Different callers, different edge cases.
- HSSS reports differences among the selected samples; with the reference strain omitted
  from the selection it does not see sample-vs-reference differences at all.
- HSSS's `nonSynCount` excludes stop-gained (section 2); `n_missense` is SnpEff `sev=5`.

Record the observed correlation so a future reader does not mistake the divergence for a
bug.

**Coverage regression.** Assert that non-coding genes still appear in results with a
blank CDS density and a populated span density, and that they are excluded only once the
density filter is narrowed.

## 6. Files touched

`ApiCommonWebService`
- `WSFPlugin/src/main/java/org/apidb/apicomplexa/wsfplugin/highspeedsnpsearch/FindGenesWithSnpCharsPlugin.java`
  — locations SQL, `makeResultRow`, `getColumns`, `COLUMN_*` constants, unit-test filters
- `HighSpeedSnpSearch/bin/hsssGeneCharacteristicsFilter` — parse 7 columns, compute both
  densities and the normalized ratio, print 9 fields
- `HighSpeedSnpSearch/lib/perl/HsssGeneCharsFilterScriptGenerator.pm` — usage text
  (including the count-vs-percent error)
- `HighSpeedSnpSearch/bin/hsssTestSuite` — extended assertions

`ApiCommonModel`
- `Model/lib/wdk/model/questions/queries/geneQueries.xml` — `wsColumn span_snp_density`
- `Model/lib/wdk/model/questions/geneQuestions.xml` — dynamic attribute for the new
  column, display names and help per section 4.1, description per 4.3
- `Model/lib/wdk/model/questions/params/geneParams.xml` — prompts and help per 4.2

Deployment note: the plugin is a jar. A model rebuild alone does not pick up the Java or
perl changes; both repos must ship together, or the 9-field output will meet an 8-field
assertion.
