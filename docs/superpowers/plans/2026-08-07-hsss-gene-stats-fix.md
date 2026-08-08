# HSSS Gene Statistics Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the two reported statistics in the `GenesByNgsSnps` search compute what their labels claim — SNVs per kb of coding sequence, and a site-normalized dN/dS — and relabel every string that described the old behaviour.

**Architecture:** Numerators stay sample-set-dependent and are computed by HSSS from the selected samples. Denominators (CDS length, Nei-Gojobori synonymous/nonsynonymous site counts) are gene properties already derived once from the genetic code in `apidbtuning.GeneVariationSummary`; the plugin reads them in the SQL it already runs and passes them to the perl filter through `geneLocations.txt`. Both searches then rest on one definition of a synonymous site.

**Tech Stack:** Java (WSF plugin), Perl (HSSS stream filter), bash (HSSS test suite), WDK model XML, PostgreSQL.

**Spec:** `ApiCommonWebService/docs/superpowers/specs/2026-08-07-hsss-gene-stats-fix-design.md`

---

## Before you start

**Do NOT create a git worktree for this work.** These repos are checked out at
`~/workspaces/plasmodb/` and a mutagen session carries that working tree to the build
host. A worktree lives outside the synced path, so anything built or run on the remote
would be testing stale code. Edit in place.

Branches already exist and carry prior related commits — use them, do not branch again:

| repo | path | branch |
|---|---|---|
| ApiCommonWebService | `~/workspaces/plasmodb/ApiCommonWebService` | `feature/hsss-noncoding-class` |
| ApiCommonModel | `~/workspaces/plasmodb/ApiCommonModel` | `dnaseq-merge-experiments` |

Two environment facts you will need:

- The appDb is reachable read-only at `psql -h localhost -p 5432 -d unidb_shu_a`.
- Remote builds run through `bin/veup-build.sh plasmodb wb model` from
  `~/workspaces/agentic-veupath-dev`. **Flags go before the profile name.**

**Deployment coupling:** the perl output grows from 8 fields to 9 and the Java asserts on
the count. The two repos must ship together. Do not deploy one without the other.

---

## Known remaining breakage in hsssTestSuite (deliberately NOT fixed here)

Found while repairing the suite for Task 0. Recorded so the next person does not
rediscover them; each is out of scope for the statistics work.

- **`hsssTestSuite:135` — the majorAlleles stage is dead.** It calls
  `hsssGenerateMajorAllelesScript` with 12 arguments where the generator requires 14-15,
  so it dies in `usage()`. Same root cause as the four sites Task 0 fixed: the missing
  `hsssReconstructSnpId Variant_ NULL` triple. Not fixed because
  `expected/majorAlleles.txt` holds raw contig indices (`99 2011 C ...`) rather than
  `Variant_e99_2011` ids, so it predates the reconstruct step being wired in at all —
  repairing it means re-baselining a stage whose output has never been validated, which
  is a larger and separate piece of work.

- **`expected/polymorphismSearch.txt` is orphaned and self-contradictory.** Referenced
  nowhere in the suite, and it disagrees with its sibling
  `expected/polymorphismSearchWithSourceIds.txt` about which SNPs are non-synonymous (it
  marks contigs 80/99/102; the sibling marks 80/102/103). Two stale files from different
  eras. Probably wants deleting, but deleting a fixture is not a statistics fix.

- **`hsssReconstructSnpId:59` documents the wrong encoding.** Its usage text says
  `product_class(-1=noncoding,0=syn,1=nonsyn,2=nonsense)`; the code immediately above it
  maps `0` to `non-coding`, `1` to `syn`, `2` to `non-syn`, and negatives to
  `has stop codon`. The same class of defect as the labels this plan corrects —
  documentation describing behaviour the code does not have.

---

### Task 0: Repair hsssTestSuite (REPLACES the original Task 1)

**This task was added after the plan was written.** The original Task 1 assumed the suite
ran the geneChars filter and discarded the result. It does not: `extractArgs` consumes
five arguments and `getFinalCommandString` unpacks fourteen, but the suite supplied
eleven, so every argument shifted left by three and the gene locations file was never
passed — the filter received the literal string `5` in its place. The stage has never
produced meaningful output, which is why its `diff` was commented out.

Three sub-parts, executed in this order:

- **0a — fix the arguments.** Insert `strainsList.txt hsssReconstructSnpId Variant_ NULL`
  in the slot after `strains_list_file` at all four call sites
  (`hsssTestSuite:41,59,76,93`). Sites 41/59/76 genuinely use `idPrefix`/`idSuffix`;
  geneChars emits gene ids and never reads them, so there they are inert positional
  filler. Committed as `5dfe387`, touching `hsssTestSuite` only.

- **0b — characterize the stale baselines.** Three expected files diverge from actual
  output in exactly two classes, both traced and both approved: `%d` -> `%.1f` on the
  percentage columns (`d3771af`, 2014-07-26) and the product-class column going from a
  boolean `y`/blank to the four-value label set (`f1ac0d9`, 2014-08-19). Neither commit
  updated `test/expected/`. Row counts, field counts and column 1 are identical
  throughout; there are no unexplained differences.

- **0c/0d — extend the fixture, then baseline once.** The fixture produces only `syn` and
  `non-syn`; it contains no non-coding position and no stop codon, so
  `nonCodingCount` and `nonsenseCount` are 0 for every gene. Re-baselining before fixing
  that would produce a green suite that never executes the paths this branch changes.
  So: add a non-coding position and a stop-codon position to `test/textData/strain*.txt`
  and `referenceGenome.txt`, with at least one falling inside a gene span in
  `geneFilters.txt`, THEN regenerate all four expected files together — the three above
  plus the new `geneCharsFilter.txt`. Uncomment the geneChars assertion. Prove it can
  fail by corrupting the expected file and confirming a non-zero exit.

  geneChars needs widened arguments to emit anything: the suite's current
  `coding 2 5 .1 .9 3 1000` yields empty output; use `all 0 -1 0 -1 0 -1`.

  Also convert `test/textData/geneFilters.txt` to unix line endings — `chomp` strips only
  `\n`, so the trailing `\r` lands on the last field, and Task 2 appends numeric columns
  after it.

---

### Task 1: (SUPERSEDED — folded into Task 0d)

Kept for numbering. The baseline capture and assertion uncommenting described below now
happen in Task 0d, against the extended fixture. Read this section for the
`hsssTestSuite` edit and the fail-proof step, but do not execute it separately.

The suite runs `hsssGeneCharacteristicsFilter` today but asserts nothing — the `diff` is
commented out and the expected file does not exist. Establish the baseline BEFORE
changing behaviour, so the later tasks have something to break.

**Files:**
- Modify: `ApiCommonWebService/HighSpeedSnpSearch/bin/hsssTestSuite:99-105`
- Create: `ApiCommonWebService/HighSpeedSnpSearch/test/expected/geneCharsFilter.txt`
- Modify: `ApiCommonWebService/HighSpeedSnpSearch/test/textData/geneFilters.txt`

- [ ] **Step 1: Convert the fixture to unix line endings**

`geneFilters.txt` currently has CRLF endings. `chomp` strips only `\n`, so today the
trailing `\r` lands on the last field (`gene_source_id`) and is invisible. Once columns
are appended in Task 3 it would land on a numeric field and produce warnings.

Run:

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService/HighSpeedSnpSearch
sed -i 's/\r$//' test/textData/geneFilters.txt
cat -A test/textData/geneFilters.txt
```

Expected: five lines ending in `$` with no `^M`.

- [ ] **Step 2: Run the suite and capture current output as the expected file**

`hsssTestSuite` takes the working directory as its one argument, so choose it rather
than hunting for it.

Run:

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService/HighSpeedSnpSearch
export PROJECT_HOME=~/workspaces/plasmodb
rm -rf /tmp/hsssTest && mkdir -p /tmp/hsssTest
bin/hsssTestSuite /tmp/hsssTest 2>&1 | tail -20
cp /tmp/hsssTest/geneChars_result.txt test/expected/geneCharsFilter.txt
cat test/expected/geneCharsFilter.txt
```

Expected: tab-delimited rows of exactly 8 fields:
`geneId density dndsRatio synCount nonSynCount nonCodingCount nonsenseCount snpsCount`

If the file is empty, the fixture SNP stream produces no gene hits under the suite's
`coding 2 5 .1 .9 3 1000` arguments. In that case widen the arguments on
`hsssTestSuite:93` to `all 0 -1 0 -1 0 -1` so at least one gene is emitted, and use that
output. Record which arguments you used in the commit message.

- [ ] **Step 3: Uncomment the assertion**

In `bin/hsssTestSuite`, replace lines 99-105:

```bash
#echo "Comparing expected runGeneChars output with result..."
#diff $PROJECT_HOME/ApiCommonWebService/HighSpeedSnpSearch/test/expected/geneCharsFilter.txt geneChars_result.txt  
#diffStat=$?
#if [ $diffStat != 0 ]; then
#   exit -1
#fi
#echo "matched"
```

with:

```bash
echo "Comparing expected runGeneChars output with result..."
diff $PROJECT_HOME/ApiCommonWebService/HighSpeedSnpSearch/test/expected/geneCharsFilter.txt geneChars_result.txt
diffStat=$?
if [ $diffStat != 0 ]; then
   exit -1
fi
echo "matched"
```

- [ ] **Step 4: Run the suite to verify it passes**

Run:

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService/HighSpeedSnpSearch
rm -rf /tmp/hsssTest && mkdir -p /tmp/hsssTest
bin/hsssTestSuite /tmp/hsssTest; echo "exit=$?"
```

Expected: `matched` printed for geneChars, `exit=0`.

- [ ] **Step 5: Verify the test actually detects a change**

Temporarily corrupt the expected file and confirm the suite fails — a test that cannot
fail is not a test.

Run:

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService/HighSpeedSnpSearch
echo "bogus	1	2	3	4	5	6	7" >> test/expected/geneCharsFilter.txt
rm -rf /tmp/hsssTest && mkdir -p /tmp/hsssTest
bin/hsssTestSuite /tmp/hsssTest >/dev/null 2>&1; echo "exit=$?"
git checkout test/expected/geneCharsFilter.txt
```

Expected: `exit=255` (the script's `exit -1`). Then the checkout restores the file.

- [ ] **Step 6: Commit**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService
git add HighSpeedSnpSearch/bin/hsssTestSuite HighSpeedSnpSearch/test/expected/geneCharsFilter.txt HighSpeedSnpSearch/test/textData/geneFilters.txt
git commit -m "test: assert hsssGeneCharacteristicsFilter output instead of only running it

The geneChars diff in hsssTestSuite was commented out and its expected file
never existed, so the filter had no regression coverage. Captures current
output as the baseline before changing the statistics it computes.

Also converts geneFilters.txt to unix line endings: chomp strips only \\n, so
the trailing \\r was landing on the last field."
```

---

### Task 2: Widen the test fixture to carry normalizers

The fixture gains the three columns the filter will read in Task 3. Doing this first,
while the filter still ignores them, proves the parser change in Task 3 is what makes the
numbers move.

**Files:**
- Modify: `ApiCommonWebService/HighSpeedSnpSearch/test/textData/geneFilters.txt`

- [ ] **Step 1: Append cdsLen, synSites, nonsynSites to each fixture row**

Write `test/textData/geneFilters.txt` as exactly this (tab-delimited, unix endings). The
values are chosen to exercise every branch:

```
e99	1000	3000	g1	1200	300	900
f100	5	700	g2	600	150	450
g102	3001	40000	g3	0	0	0
h103	30021	40000	g4	900	0	675
j201	20	50	g5	300	75	225
```

- `g1`, `g2`, `g5`: normal coding genes — both densities and the ratio defined.
- `g3`: `cdsLen`/`synSites`/`nonsynSites` all zero — stands for a non-coding gene, where
  the tuning table has NULL. CDS density and ratio must be blank; span density populated.
- `g4`: `synSites` zero but `nonsynSites` non-zero — ratio blank, both densities defined.

Note for the engineer: the plugin writes an empty string for a SQL NULL, and the perl
tests these fields for truth, so empty string and `0` behave identically. `0` is used in
the fixture because it survives a round trip through `sort` and is visible in `cat`.

- [ ] **Step 2: Run the suite to verify it still passes**

Run:

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService/HighSpeedSnpSearch
rm -rf /tmp/hsssTest && mkdir -p /tmp/hsssTest
bin/hsssTestSuite /tmp/hsssTest; echo "exit=$?"
```

Expected: `matched`, `exit=0`. The filter splits into four scalars and discards the rest,
so output is unchanged. If this fails, the fixture has a whitespace error — check with
`cat -A`.

- [ ] **Step 3: Commit**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService
git add HighSpeedSnpSearch/test/textData/geneFilters.txt
git commit -m "test: add normalizer columns to the geneChars fixture

cdsLen, synSites, nonsynSites per gene, covering the normal case, an all-zero
non-coding gene, and a gene with zero synonymous sites. The filter ignores them
until the next commit, so output is unchanged here."
```

---

### Task 3: Compute the corrected statistics in the perl filter

**Files:**
- Modify: `ApiCommonWebService/HighSpeedSnpSearch/bin/hsssGeneCharacteristicsFilter:23,68,83-121,123-146`
- Modify: `ApiCommonWebService/HighSpeedSnpSearch/test/expected/geneCharsFilter.txt`

- [ ] **Step 1: Parse the three new columns in both split sites**

There are TWO places the gene locations line is split, and they must agree. Line 23:

```perl
my ($filterContigId, $filterStart, $filterEnd, $filterGeneId) = split(/\t/, $geneLocationLine);
```

becomes:

```perl
my ($filterContigId, $filterStart, $filterEnd, $filterGeneId,
    $filterCdsLen, $filterSynSites, $filterNonsynSites) = split(/\t/, $geneLocationLine);
```

Line 68, inside the `while` that advances past genes:

```perl
      ($filterContigId, $filterStart, $filterEnd, $filterGeneId) = split(/\t/, $geneLocationLine);
```

becomes:

```perl
      ($filterContigId, $filterStart, $filterEnd, $filterGeneId,
       $filterCdsLen, $filterSynSites, $filterNonsynSites) = split(/\t/, $geneLocationLine);
```

Missing the second one is the likeliest bug in this task: the first gene would get correct
statistics and every later gene would silently reuse the first gene's normalizers.

- [ ] **Step 2: Replace the statistic calculations in processGene**

In `sub processGene`, replace lines 88-90:

```perl
  my $nonCodingCount = $snpsCount - $codingCount;
  my $dnds = $synCount? $nonSynCount / $synCount : undef;
  my $density = $snpsCount / (($filterEnd - $filterStart) / 1000);
```

with:

```perl
  my $nonCodingCount = $snpsCount - $codingCount;

  # Densities. cdsDensity is coding variants over coding length, which is what this
  # search has always CLAIMED to report; spanDensity is what it actually reported, kept
  # under a name that admits it. cdsLen is empty for a gene with no coding sequence.
  my $cdsDensity  = $filterCdsLen ? 1000 * $codingCount / $filterCdsLen : undef;
  my $spanDensity = 1000 * $snpsCount / ($filterEnd - $filterStart);

  # dN/dS, each count normalized by the number of sites of its class (Nei-Gojobori,
  # computed from the genetic code in apidbtuning.GeneVariationSummary). Without this
  # normalization the ratio carries the genome's codon bias: the pooled synonymous-site
  # fraction in pfal3D7 is 17.49%, not the textbook ~25%, worth 1.43x on every gene.
  #
  # defined($dn), NOT $dn: a gene with zero nonsynonymous variants has dN = 0 and a real
  # ratio of 0, which is a strong purifying-selection signal and exactly what someone
  # filtering a low range wants. Truth-testing $dn would silently drop those genes.
  # $ds IS truth-tested, because zero there is a division by zero, not a result.
  my $dn   = $filterNonsynSites ? $nonSynCount / $filterNonsynSites : undef;
  my $ds   = $filterSynSites    ? $synCount    / $filterSynSites    : undef;
  my $dnds = (defined($dn) && $ds) ? $dn / $ds : undef;
```

- [ ] **Step 3: Point the dN/dS filter at the new value**

Replace lines 108-114:

```perl
  if ($dndsMin || $dndsMax != -1) {
    if ($synCount == 0 && $nonSynCount != 0) {
      return 0 unless $dndsMax == -1;
    } else {
      return 0 if $dnds < $dndsMin || ($dndsMax != -1 && $dnds > $dndsMax);
    }
  }
```

with:

```perl
  if ($dndsMin || $dndsMax != -1) {
    # An undefined ratio cannot be shown to be in range, so the gene is excluded - but
    # only because the user narrowed this filter. Leaving it alone (min 0, max -1) skips
    # this block entirely, which is what keeps non-coding genes in the result.
    if (!defined($dnds)) {
      return 0;
    } else {
      return 0 if $dnds < $dndsMin || ($dndsMax != -1 && $dnds > $dndsMax);
    }
  }
```

Note this subsumes the old `$synCount == 0 && $nonSynCount != 0` special case: zero
synonymous variants now yields `$ds == 0`, so `$dnds` is undef and the gene is excluded
whenever the filter is engaged — the same outcome the old branch produced, reached by one
rule instead of two.

- [ ] **Step 4: Point the density filter at CDS density**

Replace lines 116-118:

```perl
  if ($densityMin || $densityMax != -1) {
    return 0 if ($density < $densityMin || ($densityMax != -1 && $density > $densityMax));
  }
```

with:

```perl
  # Filters CDS density only. Span density is reported but not filterable, to avoid a
  # fifteenth and sixteenth param on a form that already carries fourteen.
  if ($densityMin || $densityMax != -1) {
    return 0 if (!defined($cdsDensity));
    return 0 if ($cdsDensity < $densityMin || ($densityMax != -1 && $cdsDensity > $densityMax));
  }
```

- [ ] **Step 5: Emit nine fields**

Replace line 120:

```perl
  print STDOUT join("\t", $filterGeneId, sprintf("%.2f",$density), $synCount ? sprintf("%.2f",$dnds) : undef, $synCount, $nonSynCount, $nonCodingCount, $nonsenseCount, $snpsCount) . "\n";
```

with:

```perl
  print STDOUT join("\t",
                    $filterGeneId,
                    defined($cdsDensity) ? sprintf("%.2f", $cdsDensity) : '',
                    sprintf("%.2f", $spanDensity),
                    defined($dnds) ? sprintf("%.4f", $dnds) : '',
                    $synCount, $nonSynCount, $nonCodingCount, $nonsenseCount, $snpsCount) . "\n";
```

Two changes beyond the added column. `undef` in a `join` produces an uninitialized-value
warning and an empty string; `''` is explicit. And `%.4f` rather than `%.2f` for the
ratio, because site normalization divides by site counts in the hundreds, so meaningful
values now sit well below 1 where two decimals would collapse them.

- [ ] **Step 6: Update the usage text**

Replace lines 131-144 of the `usage` sub. Note the existing text says the snps_min/max
are a PERCENT — they are compared against raw counts in the code, so that has always been
wrong and is corrected here.

```
  - gene_locations_filter_file: tab delimited:  contig_source_id, start, end, gene_source_id, cds_length, syn_sites, nonsyn_sites.   Must be sorted by location.  The last three may be empty for a gene with no coding sequence; the statistics that need them are then reported empty.
  - snp_class:  all, coding, noncoding, synonymous, nonsynonymous, nonsense
  - snps_min: min NUMBER of SNPs in the gene that belong to the specified class
  - snps_max: max NUMBER of SNPs in the gene that belong to the specified class
  - dnds_min: min site-normalized dN/dS ratio
  - dnds_max: max site-normalized dN/dS ratio
  - density_min: min coding SNPs per kb of CDS
  - density_max: max coding SNPs per kb of CDS

  - snp_search_result: tab_delimited where first column is contig index and second is gene location.

Replaces the first two columns of snp_search_result with a single column that is the concatenation of the contig_source_id-location, ie, a snp source id.

Outputs these columns (tab delim): geneId cdsDensity spanDensity dndsRatio synCount nonSynCount nonCodingCount nonsenseCount snpsCount
```

- [ ] **Step 7: Run the suite to verify it FAILS**

Run:

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService/HighSpeedSnpSearch
rm -rf /tmp/hsssTest && mkdir -p /tmp/hsssTest
bin/hsssTestSuite /tmp/hsssTest >/dev/null 2>&1; echo "exit=$?"
```

Expected: `exit=255`. The output now has 9 fields and different values, so the Task 1
baseline must reject it. If this passes, the assertion is not wired up — go back to
Task 1 Step 5.

- [ ] **Step 8: Inspect the new output and check it by hand**

Run:

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService/HighSpeedSnpSearch
rm -rf /tmp/hsssTest && mkdir -p /tmp/hsssTest
bin/hsssTestSuite /tmp/hsssTest >/dev/null 2>&1
cat -A /tmp/hsssTest/geneChars_result.txt
```

For each emitted row verify by hand against the fixture in Task 2:
- `cdsDensity` = `1000 * codingCount / cdsLen`, empty when `cdsLen` is 0
- `spanDensity` = `1000 * snpsCount / (end - start)`, always populated
- `dnds` = `(nonSynCount/nonsynSites) / (synCount/synSites)`, empty when `synSites` is 0
- field count is 9 on every row

Do not proceed until each row checks out. This hand-check is the real test; the expected
file only locks it in.

- [ ] **Step 9: Re-baseline the expected file**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService/HighSpeedSnpSearch
cp /tmp/hsssTest/geneChars_result.txt test/expected/geneCharsFilter.txt
rm -rf /tmp/hsssTest && mkdir -p /tmp/hsssTest
bin/hsssTestSuite /tmp/hsssTest >/dev/null 2>&1; echo "exit=$?"
```

Expected: `exit=0`.

- [ ] **Step 10: Commit**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService
git add HighSpeedSnpSearch/bin/hsssGeneCharacteristicsFilter HighSpeedSnpSearch/test/expected/geneCharsFilter.txt
git commit -m "Compute CDS density and site-normalized dN/dS in the geneChars filter

Density was total variants over GENOMIC span while claiming coding variants
over CDS length; it is now the latter, with the old value retained as a
separate span-density column. The nonsyn/syn ratio had no site normalization
and so carried the genome's codon bias - worth 1.43x in pfal3D7, where the
pooled synonymous-site fraction is 17.49% rather than the textbook ~25%.

Normalizers arrive per gene in geneLocations.txt; the filter treats an empty
one as \"statistic not defined\" and excludes the gene only when the matching
filter has actually been narrowed.

Output grows from 8 fields to 9. The Java that parses it changes in the next
commit; the two must deploy together."
```

---

### Task 4: Supply the normalizers from the plugin

**Files:**
- Modify: `ApiCommonWebService/WSFPlugin/src/main/java/org/apidb/apicomplexa/wsfplugin/highspeedsnpsearch/FindGenesWithSnpCharsPlugin.java`

- [ ] **Step 1: Add the new column constant**

After the existing `COLUMN_DENSITY` declaration, add:

```java
  public static final String COLUMN_SPAN_DENSITY = "span_snp_density";
```

Leave `COLUMN_DENSITY = "cds_snp_density"` alone. The name becomes accurate rather than
needing a rename that would break `attributesList summary=` and saved strategies.

- [ ] **Step 2: Extend the gene locations SQL**

In `initForBashScript`, replace the `String sql = ...` assignment:

```java
        String sql = "select g.sequence_id, g.start_min, g.end_max, g.source_id" + newline +
            "from webready.GeneAttributes_p g " + newline + "where g.source_id is not null" + newline +
            " and g.organism = '" + organism + "'";
```

with:

```java
        // LEFT JOIN, never inner: GeneVariationSummary holds one row per gene that has
        // COHORT variants (5,579 of 5,720 annotated pfal3D7 genes), and a gene missing
        // from it must still get a locations line and still report its counts. Only its
        // normalized statistics come back empty.
        //
        // These three columns are gene properties derived from the genetic code, not
        // from any sample set, which is why reading them here is sound: the numerators
        // stay sample-set-dependent and HSSS still computes them.
        String sql = "select g.sequence_id, g.start_min, g.end_max, g.source_id," + newline +
            "       gvs.cds_length, gvs.syn_sites, gvs.nonsyn_sites" + newline +
            "from webready.GeneAttributes_p g " + newline +
            "left join apidbtuning.GeneVariationSummary gvs" + newline +
            "       on gvs.gene_source_id = g.source_id" + newline +
            "      and gvs.project_id     = g.project_id" + newline +
            "where g.source_id is not null" + newline +
            " and g.organism = '" + organism + "'";
```

- [ ] **Step 3: Write the new columns to the locations file**

Replace the result-set loop body:

```java
          while (rs.next()) {
            String seqId = rs.getString(1);
            String start = rs.getString(2);
            String end = rs.getString(3);
            String geneId = rs.getString(4);
            bw.write(seqId + "\t" + start + "\t" + end + "\t" + geneId);
            bw.newLine();
          }
```

with:

```java
          while (rs.next()) {
            String seqId = rs.getString(1);
            String start = rs.getString(2);
            String end = rs.getString(3);
            String geneId = rs.getString(4);
            // getString returns null for a SQL NULL; the filter tests these for truth,
            // so an empty string reads as "no normalizer" exactly like a zero would.
            String cdsLen = rs.getString(5) == null ? "" : rs.getString(5);
            String synSites = rs.getString(6) == null ? "" : rs.getString(6);
            String nonsynSites = rs.getString(7) == null ? "" : rs.getString(7);
            bw.write(seqId + "\t" + start + "\t" + end + "\t" + geneId + "\t"
                + cdsLen + "\t" + synSites + "\t" + nonsynSites);
            bw.newLine();
          }
```

The `apiSortNoLocale -k 1,1 -k 2,2n` that follows is unaffected — the new columns are
appended after the sort keys.

- [ ] **Step 4: Extend the unit-test filter rows**

In the same method, replace the `"unit test"` branch's `testFilters`:

```java
        String[] testFilters = new String[] { "e99\t1000\t3000\tg1", "f100\t500\t700\tg2",
            "h103\t30021\t40000\tg3", "j201\t20\t50\tg4" };
```

with:

```java
        String[] testFilters = new String[] {
            "e99\t1000\t3000\tg1\t1200\t300\t900",
            "f100\t500\t700\tg2\t600\t150\t450",
            "h103\t30021\t40000\tg3\t\t\t",      // no coding sequence: normalizers empty
            "j201\t20\t50\tg4\t900\t0\t675" };   // zero synonymous sites: ratio undefined
```

- [ ] **Step 5: Accept nine fields and map the new column**

Replace `makeResultRow` in full:

```java
  protected String[] makeResultRow(String[] parts, Map<String, Integer> columns, String projectId)
      throws PluginModelException {
    if (parts.length != 9)
      throw new PluginModelException("Wrong number of columns in results file.  Expected 9, found " +
          parts.length);

    String[] row = new String[12];
    row[columns.get(COLUMN_GENE_SOURCE_ID)] = parts[0];
    row[columns.get(COLUMN_SOURCE_ID)] = null;
    row[columns.get(COLUMN_PROJECT_ID)] = projectId;
    row[columns.get(COLUMN_MATCHED_RESULT)] = "Y";
    row[columns.get(COLUMN_DENSITY)] = parts[1];
    row[columns.get(COLUMN_SPAN_DENSITY)] = parts[2];
    row[columns.get(COLUMN_DNDS)] = parts[3];
    row[columns.get(COLUMN_SYN)] = parts[4];
    row[columns.get(COLUMN_NONSYN)] = parts[5];
    row[columns.get(COLUMN_NONCODING)] = parts[6];
    row[columns.get(COLUMN_NONSENSE)] = parts[7];
    row[columns.get(COLUMN_TOTAL)] = parts[8];
    return row;
  }
```

`new String[12]`, up from 11, because the row array is indexed by the column map and one
column was added.

- [ ] **Step 6: Declare the new column to the framework**

Replace `getColumns`:

```java
  public String[] getColumns(PluginRequest request) {
    return new String[] { COLUMN_GENE_SOURCE_ID, COLUMN_PROJECT_ID, COLUMN_DENSITY, COLUMN_SPAN_DENSITY,
        COLUMN_DNDS, COLUMN_SYN, COLUMN_NONSYN, COLUMN_NONCODING, COLUMN_NONSENSE, COLUMN_TOTAL };
  }
```

- [ ] **Step 7: Verify it compiles**

Run:

```bash
ssh cedar 'bash -lc "cd /var/www/PlasmoDB/plasmo.jbrestel/project_home/ApiCommonWebService && mvn -q -pl WSFPlugin -am compile 2>&1 | tail -20"'
```

Expected: no output, or `BUILD SUCCESS`. If the module coordinates differ, fall back to
`mvn -q compile` at the repo root. A compile error naming `COLUMN_SPAN_DENSITY` means
Step 1 was skipped.

- [ ] **Step 8: Commit**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService
git add WSFPlugin/src/main/java/org/apidb/apicomplexa/wsfplugin/highspeedsnpsearch/FindGenesWithSnpCharsPlugin.java
git commit -m "Feed per-gene normalizers to the geneChars filter and read its ninth column

The gene locations query gains cds_length, syn_sites and nonsyn_sites from
apidbtuning.GeneVariationSummary via LEFT JOIN, so both this search and
GenesByVariantCharacteristics rest on one definition of a synonymous site
rather than two that can drift.

makeResultRow now expects 9 fields and maps the new span_snp_density column.
This commit and the previous one must deploy together."
```

---

### Task 5: Declare the new column in the model

**Files:**
- Modify: `ApiCommonModel/Model/lib/wdk/model/questions/queries/geneQueries.xml:2944`

- [ ] **Step 1: Add the wsColumn**

In the `GenesByNgsSnps` processQuery — the one at line ~2944, NOT the chip query at 2795
— add a line after `<wsColumn name="cds_snp_density" columnType="float"/>`:

```xml
      <wsColumn name="span_snp_density" columnType="float"/>
```

Verify you edited the right one:

```bash
cd ~/workspaces/plasmodb/ApiCommonModel/Model/lib/wdk
grep -n -B40 'wsColumn name="span_snp_density"' model/questions/queries/geneQueries.xml | grep 'processQuery name='
```

Expected: `<processQuery name="GenesByNgsSnps"`.

- [ ] **Step 2: Build the model**

Run: `cd ~/workspaces/agentic-veupath-dev && bin/veup-build.sh plasmodb wb model 2>&1 | tail -5`
Expected: `OK - Reloaded application at context path [/plasmo.jbrestel]`.

- [ ] **Step 3: Commit**

```bash
cd ~/workspaces/plasmodb/ApiCommonModel
git add Model/lib/wdk/model/questions/queries/geneQueries.xml
git commit -m "Declare span_snp_density on the GenesByNgsSnps process query"
```

---

### Task 6: Honest labels on the result columns

**Files:**
- Modify: `ApiCommonModel/Model/lib/wdk/model/questions/geneQuestions.xml:1475-1476,1512-1563`

The chip-SNP question above this one is commented out (`geneQuestions.xml:1373`), so
these attribute names are owned by `GenesByNgsSnps` alone — no cross-question impact.

- [ ] **Step 1: Add the new column and relabel the existing ones**

Inside the `<dynamicAttributes>` block of `GenesByNgsSnps` (lines 1512-1563), apply
exactly these display-name and help changes, and add one new `columnAttribute`:

```xml
	     <columnAttribute name="cds_snp_density" displayName="SNVs per kb (CDS)" align="center"
	       help="Coding variants per kilobase of coding sequence, using the representative transcript's CDS length. Blank for genes with no coding sequence.">
	        <reporter name="histogram" displayName="Histogram" scopes=""
                  implementation="org.gusdb.wdk.model.report.reporter.HistogramAttributeReporter">
                  <description>Display the histogram of the values of this attribute</description>
                  <property name="type">float</property>
                </reporter>
          </columnAttribute>
	     <columnAttribute name="span_snp_density" displayName="SNVs per kb (gene span)" align="center"
	       help="All variants in the gene divided by its genomic length, introns and UTRs included.">
	        <reporter name="histogram" displayName="Histogram" scopes=""
                  implementation="org.gusdb.wdk.model.report.reporter.HistogramAttributeReporter">
                  <description>Display the histogram of the values of this attribute</description>
                  <property name="type">float</property>
                </reporter>
          </columnAttribute>
	     <columnAttribute name="ngs_dn_ds_ratio" displayName="dN/dS (site-normalized)" align="center"
	       help="Nonsynonymous and synonymous counts each divided by the number of sites of that class, from Nei-Gojobori counts over the representative transcript. Below 1 suggests purifying selection. Stop-gained variants are counted as nonsense rather than nonsynonymous, so they do not enter the numerator. This is a count ratio - it weights a rare variant the same as a common one - so it will not equal the piN/piS in the SNV Characteristics search.">
	        <reporter name="histogram" displayName="Histogram" scopes=""
                  implementation="org.gusdb.wdk.model.report.reporter.HistogramAttributeReporter">
                  <description>Display the histogram of the values of this attribute</description>
                  <property name="type">float</property>
                </reporter>
          </columnAttribute>
```

Note the `<property name="type">` values change from `int` to `float` on all three —
they were `int` on values that have always been fractional.

Then change these four display names in place, leaving their `reporter` blocks as they
are:

| line ~ | from | to | add help |
|---|---|---|---|
| `ngs_total_snps` | `Total SNPs` | `SNVs in gene span` | `Variant positions anywhere between the gene's start and end.` |
| `ngs_num_synonymous` | `Synonymous SNPs` | `Synonymous SNVs` | *(none)* |
| `ngs_num_non_synonymous` | `Nonsynonymous SNPs` | `Missense SNVs` | `Amino-acid-changing, excluding those that introduce a stop.` |
| `num_nonsense` | `Nonsense SNPs` | `Stop-gained SNVs` | replace the existing typo help `SNPs where one or more variants encodes a stop coding` with `Variants where one or more alleles encodes a premature stop codon.` |
| `num_noncoding` | `Non-coding SNPs` | `Unclassified SNVs` | `No protein product could be assigned: positions outside coding sequence, and positions where the reference product was unavailable.` |

- [ ] **Step 2: Add the new column to the summary attribute list**

Replace lines 1475-1476:

```xml
        <attributesList
				              summary="gene_product,ngs_total_snps,ngs_num_non_synonymous,ngs_num_synonymous,num_nonsense,num_noncoding,ngs_dn_ds_ratio,cds_snp_density"
				              sorting="ngs_total_snps desc,cds_snp_density desc"
        /> 
```

with:

```xml
        <attributesList
				              summary="gene_product,ngs_total_snps,ngs_num_non_synonymous,ngs_num_synonymous,num_nonsense,num_noncoding,ngs_dn_ds_ratio,cds_snp_density,span_snp_density"
				              sorting="cds_snp_density desc,ngs_total_snps desc"
        /> 
```

- [ ] **Step 3: Build the model**

Run: `cd ~/workspaces/agentic-veupath-dev && bin/veup-build.sh plasmodb wb model 2>&1 | tail -5`
Expected: `OK - Reloaded application`.

- [ ] **Step 4: Verify the labels through the service**

From an already-authenticated tab on `https://jbrestel.plasmodb.org` (a raw curl
307-redirects to autologin), run in the browser console:

```js
const s = await (await fetch('/plasmo.jbrestel/service/record-types/transcript/searches/GenesByNgsSnps?expandParams=true')).json();
(s.searchData?.dynamicAttributes ?? s.dynamicAttributes ?? []).map(a => a.name + ' | ' + a.displayName)
```

Expected to include `cds_snp_density | SNVs per kb (CDS)`, `span_snp_density | SNVs per kb (gene span)`, and `ngs_dn_ds_ratio | dN/dS (site-normalized)`.

- [ ] **Step 5: Commit**

```bash
cd ~/workspaces/plasmodb/ApiCommonModel
git add Model/lib/wdk/model/questions/geneQuestions.xml
git commit -m "Label the GenesByNgsSnps result columns for what they now compute

Adds SNVs per kb (gene span) alongside the now-actually-CDS density, renames
the ratio to say it is site-normalized, and corrects three labels that never
matched the classifier: nonsynonymous excludes stop-gained, and non-coding is
really unclassified - class 0 means no product byte was available, which
covers unclassifiable positions as well as genuinely non-coding ones."
```

---

### Task 7: Honest labels on the params

**Files:**
- Modify: `ApiCommonModel/Model/lib/wdk/model/questions/params/geneParams.xml:2328-2341,2350-2362,2379-2391,2506+`

- [ ] **Step 1: Relabel the density pair (lines 2379-2391)**

Replace both `stringParam` blocks:

```xml
    <stringParam name="snp_density_lower"
                 prompt="SNVs per kb (CDS) &gt;= "
                 number="true">
      <help>Coding variants per kilobase of coding sequence. Genes with no coding sequence have no value here and are returned only while this filter is left alone.</help>
      <suggest default="0"/>
    </stringParam>

    <stringParam name="snp_density_upper"
                 prompt="SNVs per kb (CDS) &lt;= "
                 number="true">
      <help>Coding variants per kilobase of coding sequence.  NOTE:  Leaving this parameter value empty means you don't care what the upper bound is.</help>
      <suggest allowEmpty="true" emptyValue="-1"/>
    </stringParam>
```

- [ ] **Step 2: Relabel the ratio pair (lines 2328-2341)**

```xml
    <stringParam name="dn_ds_ratio_upper"
                 prompt="dN/dS (site-normalized) &lt;= "
                 number="true">
      <help>Upper bound on the site-normalized dN/dS ratio.  NOTE:  Leaving this parameter value empty means you don't care what the upper bound is.</help>
      <suggest allowEmpty="true" emptyValue="-1"/>
    </stringParam>

    <stringParam name="dn_ds_ratio_lower"
                 prompt="dN/dS (site-normalized) &gt;= "
                 number="true">
      <help>Nonsynonymous and synonymous counts each divided by the number of sites of that class. Below 1 suggests purifying selection. Stop-gained variants are counted as nonsense rather than nonsynonymous and so do not enter the numerator. Genes with no synonymous sites have no value and are returned only while this filter is left alone.</help>
      <suggest default="0"/>
    </stringParam>
```

- [ ] **Step 3: Relabel the occurrence pair (lines 2350-2362)**

Only the word SNP changes; the count wording was already correct.

```xml
    <stringParam name="occurrences_upper"
                 prompt="Number of SNVs of the selected class &lt;= "
                 number="true">
      <help>Upper Bound on the number of SNVs of the selected class.  NOTE:  Leaving this parameter value empty means you don't care what the upper bound is.</help>
      <suggest allowEmpty="true" emptyValue="-1"/>
    </stringParam>

    <stringParam name="occurrences_lower"
                 prompt="Number of SNVs of the selected class &gt;= "
                 number="true">
      <help>Lower Bound on the number of SNVs of the selected class</help>
      <suggest default="0"/>
    </stringParam>
```

- [ ] **Step 4: Relabel the class enum (line ~2506)**

Change the prompt and three enum TERMS. The `<internal>` values are a contract with
`FindGenesWithSnpCharsPlugin.legalParams` and the filter's branches — do NOT touch them.

```xml
    <enumParam name="snp_class"
               prompt="SNV Class"
               multiPick="false"
               quote="false">
      <noTranslation value="true" includeProjects="EuPathDB" />

      <help>
        Choose the class of SNV you want to query on ... choose minumum and maximum numbers below
      </help>
      <enumList>
        <enumValue>
          <term>All SNVs</term>
          <internal>all</internal>
        </enumValue>
        <enumValue>
          <term>Coding</term>
          <internal>coding</internal>
        </enumValue>
        <enumValue>
          <term>Unclassified</term>
          <internal>noncoding</internal>
        </enumValue>
        <enumValue>
          <term>Missense</term>
          <internal>nonsynonymous</internal>
        </enumValue>
        <enumValue>
          <term>Stop-gained</term>
          <internal>nonsense</internal>
        </enumValue>
        <enumValue>
          <term>Synonymous</term>
          <internal>synonymous</internal>
        </enumValue>
      </enumList>
    </enumParam>
```

- [ ] **Step 5: Build and verify the prompts**

Run: `cd ~/workspaces/agentic-veupath-dev && bin/veup-build.sh plasmodb wb model 2>&1 | tail -5`
Expected: `OK - Reloaded application`.

Then from an authenticated browser tab:

```js
const s = await (await fetch('/plasmo.jbrestel/service/record-types/transcript/searches/GenesByNgsSnps?expandParams=true')).json();
(s.searchData?.parameters ?? s.parameters).map(p => p.name + ' | ' + p.displayName)
```

Expected to include `snp_density_lower | SNVs per kb (CDS) >= ` and
`dn_ds_ratio_lower | dN/dS (site-normalized) >= `.

- [ ] **Step 6: Commit**

```bash
cd ~/workspaces/plasmodb/ApiCommonModel
git add Model/lib/wdk/model/questions/params/geneParams.xml
git commit -m "Label the GenesByNgsSnps params for what they now filter

Density is CDS density, the ratio is site-normalized, and the class enum terms
follow the corrected column names. Internal enum values are unchanged - they
are a contract with legalParams and with the filter's branches."
```

---

### Task 8: Update the question description

**Files:**
- Modify: `ApiCommonModel/Model/lib/wdk/model/questions/geneQuestions.xml` (the two `<description>` blocks of `GenesByNgsSnps`)

- [ ] **Step 1: Replace the codon-bias promise in the PlasmoDB description**

Find this bullet:

```html
		  <li>Due to the extreme codon bias in the <i>P. falciparum</i> genome, the ratio of non-synonymous to synonymous SNPs within each gene is much higher than expected. This should be considered when creating queries. We are intending to calculate more reliable normalized Dn/Ds or Ka/Ks ratios in subsequent releases of PlasmoDB.</li>
```

Replace with:

```html
		  <li>The dN/dS ratio reported here IS normalized by synonymous and nonsynonymous site counts, derived from the genetic code over the gene's representative transcript, so it does not carry the codon-bias inflation that a raw count ratio does. In <i>P. falciparum</i> the pooled synonymous-site fraction is 17.49% rather than the textbook ~25%, which is a 1.43x correction on every gene.</li>
		  <li>It remains a count ratio: a variant seen in one sample weights the same as one at 50% frequency. For a frequency-weighted piN/piS across every sample loaded, use the "SNV Characteristics" search.</li>
```

- [ ] **Step 2: Add the scope bullet to BOTH descriptions**

There are two `<description>` blocks, one `includeProjects="PlasmoDB"` and one for the
other projects. Add this bullet as the first `<li>` of each:

```html
		  <li><b>These statistics are computed over the samples you select</b>, so they change with your sample set and are not comparable to the precomputed values on the gene record page or in the "SNV Characteristics" search.</li>
```

- [ ] **Step 3: Build**

Run: `cd ~/workspaces/agentic-veupath-dev && bin/veup-build.sh plasmodb wb model 2>&1 | tail -5`
Expected: `OK - Reloaded application`.

- [ ] **Step 4: Commit**

```bash
cd ~/workspaces/plasmodb/ApiCommonModel
git add Model/lib/wdk/model/questions/geneQuestions.xml
git commit -m "Point the GenesByNgsSnps description at the normalized ratio

The PlasmoDB description promised normalized Dn/Ds in a future release; it is
here. Also states in both descriptions that these statistics are sample-set
scoped and so not comparable to the precomputed ones."
```

---

### Task 9: Whole-cohort cross-check against the tuning table

This is the acceptance test. It cannot be an equality assertion — the two pipelines use
different classifiers — so it is a correlation check with the divergence explained.

**Files:**
- Create: `ApiCommonWebService/docs/superpowers/specs/2026-08-07-hsss-gene-stats-validation.md`

- [ ] **Step 1: Get the tuning table's site-normalized ratio**

Run:

```bash
psql -h localhost -p 5432 -d unidb_shu_a -P pager=off -c "
select gene_source_id,
       round(((n_missense::numeric / nullif(nonsyn_sites,0))
            / nullif(n_synonymous::numeric / nullif(syn_sites,0), 0))::numeric, 4) as dnds_gvs
from apidbtuning.genevariationsummary
where org_abbrev = 'pfal3D7' and n_synonymous > 0
order by gene_source_id
limit 20;"
```

Expected: 20 rows with `dnds_gvs` values, most below 2.

- [ ] **Step 2: Run the search with every sample selected**

In an authenticated browser tab, run the search through the service with the organism set
to *Plasmodium falciparum 3D7*, every sample in `variation_sample_meta`, and all filters
at their permissive defaults (`occurrences_lower=0`, `occurrences_upper=-1`,
`dn_ds_ratio_lower=0`, `dn_ds_ratio_upper=-1`, `snp_density_lower=0`,
`snp_density_upper=-1`, `snp_class=all`). Export `primary_key` and `ngs_dn_ds_ratio`.

This run takes minutes — it is HSSS over every sample. Expect that.

- [ ] **Step 3: Compare and record**

Save the search export as `/tmp/hsss_dnds.tsv` with two columns (gene id, ratio) and no
header, then compute the correlation in the database rather than by hand:

```bash
psql -h localhost -p 5432 -d unidb_shu_a -P pager=off <<'SQL'
CREATE TEMP TABLE hsss_dnds (gene_source_id text, dnds_hsss numeric);
\copy hsss_dnds FROM '/tmp/hsss_dnds.tsv' WITH (FORMAT csv, DELIMITER E'\t')
SELECT count(*) AS genes_compared,
       round(corr(rank_h, rank_g)::numeric, 3) AS spearman_rho,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY dnds_hsss)::numeric, 4) AS median_hsss,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY dnds_gvs)::numeric, 4)  AS median_gvs
FROM (
  SELECT h.dnds_hsss,
         (g.n_missense::numeric / nullif(g.nonsyn_sites,0))
       / nullif(g.n_synonymous::numeric / nullif(g.syn_sites,0), 0) AS dnds_gvs,
         rank() OVER (ORDER BY h.dnds_hsss) AS rank_h,
         rank() OVER (ORDER BY (g.n_missense::numeric / nullif(g.nonsyn_sites,0))
                             / nullif(g.n_synonymous::numeric / nullif(g.syn_sites,0), 0)) AS rank_g
  FROM hsss_dnds h
  JOIN apidbtuning.genevariationsummary g
    ON g.gene_source_id = h.gene_source_id AND g.org_abbrev = 'pfal3D7'
  WHERE h.dnds_hsss IS NOT NULL
    AND g.n_synonymous > 0 AND g.syn_sites > 0 AND g.nonsyn_sites > 0
) t;
SQL
```

Then write
`2026-08-07-hsss-gene-stats-validation.md` recording:
- the observed correlation and the number of genes compared
- the median of each, side by side
- the three reasons they differ, verbatim from the spec section 5: different classifiers
  (reference product bytes vs SnpEff severity), HSSS not seeing sample-vs-reference
  differences unless the reference strain is selected, and HSSS excluding stop-gained
  from its nonsynonymous count

**Acceptance:** Spearman rho above 0.7. Below that, something is wrong beyond classifier
differences — stop and investigate rather than recording a bad number.

- [ ] **Step 4: Coverage regression**

Confirm non-coding genes survive. Pick a pfal gene with `cds_length IS NULL`:

```bash
psql -h localhost -p 5432 -d unidb_shu_a -Atc "
select gene_source_id from apidbtuning.genevariationsummary
where org_abbrev='pfal3D7' and cds_length is null limit 5;"
```

Then verify in the Step 2 result that such a gene appears with an EMPTY
`cds_snp_density` and a POPULATED `span_snp_density`, and that narrowing
`snp_density_lower` to any value above 0 removes it.

- [ ] **Step 5: Commit**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService
git add docs/superpowers/specs/2026-08-07-hsss-gene-stats-validation.md
git commit -m "Record the whole-cohort validation of the corrected geneChars statistics"
```

---

### Task 10: Release note

**Files:**
- Modify: `ApiCommonWebsite/Model/lib/xml/PlasmoDB/news.xml`

- [ ] **Step 1: Add the note**

Saved strategies filtering on either statistic will return different results after
deploy, for every gene rather than only edge cases.

The file is a `<xmlAnswer>` of `<record>` elements. Insert this as the FIRST `<record>`
after the opening `<xmlAnswer>` and the commented-out template, and set the date to the
release date rather than today:

```xml
    <record>
       <attribute name="headline">
         <![CDATA[
         Corrected statistics in the sample-set SNV search
         ]]>
         </attribute>
       <attribute name="date">DD Mmm YYYY 00:00</attribute>
       <attribute name="tag"></attribute>
       <attribute name="category"></attribute>
       <attribute name="item">
         <![CDATA[
         <div style="margin-left: 3em;">
           <ul>
             <li>The <b>SNV Characteristics Within a Group of Samples</b> search (formerly
             "SNP Characteristics") now reports SNVs per kb of <b>coding</b> sequence. It
             previously reported all variants per kb of genomic span while labelling the
             column as CDS. The previous value is still available in a new
             "SNVs per kb (gene span)" column.</li>
             <li>Its dN/dS ratio is now normalized by synonymous and nonsynonymous site
             counts derived from the genetic code, so it no longer carries the codon-bias
             inflation of a raw count ratio.</li>
             <li><b>Both values change for every gene</b>, and saved strategies that
             filter on either will return different results than before.</li>
           </ul>
         </div>
         ]]>
       </attribute>
    </record>
```

- [ ] **Step 2: Commit**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebsite
git add Model/lib/xml/PlasmoDB/news.xml
git commit -m "News: corrected density and dN/dS in the sample-set SNV search"
```

---

## Deployment checklist

- [ ] `ApiCommonWebService` and `ApiCommonModel` merged and released together. A model-only
      deploy leaves 8-field Java parsing 9-field perl output and every search fails.
- [ ] The WSF plugin jar is rebuilt — `wb model` does not do this.
- [ ] `bin/veup-git-sync.sh plasmodb` after any local branch switch, per the repo CLAUDE.md.
