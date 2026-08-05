# HSSS variation plumbing — design

**Date:** 2026-08-05
**Status:** approved
**Scope:** Make the HighSpeedSnpSearch (HSSS) plugins emit **variation** record IDs and
read the **variation** HSSS directory layout. No WDK model XML, no new searches.
**Implementation targets:** `ApiCommonWebService` (Java, Perl, test fixtures) **and**
`ApiCommonWebsite` (one Conifer variable — see §3.3). Both on branch
`dnaseq-merge-experiments`.
**Followed by:** a separate spec for `VariationsByIsolateGroup`, the first search to
consume this. See §7.

## 1. Purpose

The `variation` record has one search, `VariationBySourceId` (a plain `sqlQuery`). Every
remaining search ported from the deprecated `snp` record — `ByIsolateGroup`,
`ByLocation`, `ByGeneIds`, `ByTwoIsolateGroups` — is a `processQuery` against an HSSS
plugin, and **none of them can work until the plugins speak the variation record's
language.**

Two mismatches block them. Both are in `ApiCommonWebService`, and both are invisible
until a search actually runs:

| | HSSS emits / expects today | variation record needs |
|---|---|---|
| result `source_id` | `NGS_SNP.Pf3D7_01_v3.29514` | `Variant_Pf3D7_01_v3_29514` |
| data directory | `<mirror>/<project>/build-N/Pfalciparum3D7/**highSpeedSnpSearch**` | `.../Pfalciparum3D7/**dnaseq**` |

The ID mismatch is the dangerous one: the plugin would return rows whose `source_id`
matches no variation record, so the search yields **zero results and no error**. The
directory mismatch fails loudly (`"Organism dir does not exist"`).

This spec fixes both, and is verifiable on its own — see §5. It is deliberately separated
from the search that consumes it, because this is a Java/Perl integration whose acceptance
test is "the IDs come out right", while the search is declarative XML that cannot be
verified until this is deployed.

## 2. Why editing in place is safe

The HSSS plugins are **entirely unused by the live model**. Verified against the
assembled PlasmoDB model (`wdkXml -model PlasmoDB`, 17,638 lines):

- zero references to `FindPolymorphismsPlugin`, `FindPolymorphismsWithSeqFilterPlugin`,
  `FindSnpsByGeneIdsPlugin`, `FindMajorAllelesPlugin`, `FindGenesWithSnpCharsPlugin`,
  `FindChipPolymorphismsPlugin`
- no `SnpQuestions` and no `SnpChipQuestions` questionSet

The reason is that the snp and snp-chip `<import>` blocks are commented out in the
**shared** `ApiCommonModel/Model/lib/wdk/apiCommonModel.xml`, so this holds for **every**
project, not just PlasmoDB. There is therefore no live consumer to regress, and no need
for a plugin subclass or a parallel script: the existing classes are modified directly and
become variation-only going forward.

> Note for anyone re-checking this: `grep` for the `<import>` line is **not** a valid test,
> because it matches inside XML comment blocks just as happily as outside them. Ask the
> assembled model instead.

Orphaned ontology rows for `GeneQuestions.GenesByNgsSnps` and `GeneQuestions.GenesBySnps`
still exist in `individuals.txt`, naming questions the model no longer defines. They are
harmless (the category ontology is not validated against the model) and out of scope here.

## 3. The changes

Three production edits across two repos, plus the test updates in §4:

| # | repo / file | change |
|---|---|---|
| 3.1 | `ApiCommonWebService` — `WSFPlugin/.../highspeedsnpsearch/HighSpeedSnpSearchAbstractPlugin.java:196` | `getSearchDir()` → `/dnaseq` |
| 3.2 | `ApiCommonWebService` — `HighSpeedSnpSearch/bin/hsssReconstructSnpId:42-43` | ID separator `.` → `_`, both joins |
| 3.3 | `ApiCommonWebsite` — `Model/lib/conifer/roles/conifer/vars/ApiCommon/default.yml:102` | `highspeedsnpsearchconfig_idPrefix` → `Variant_` |

### 3.1 Search directory — `HighSpeedSnpSearchAbstractPlugin.java:196`

```java
protected String getSearchDir() {
    return "/dnaseq";          // was "/highSpeedSnpSearch"
}
```

`findOrganismDir` (same file, :201-213) composes
`webSvcPath.replaceAll("PROJECT_GOES_HERE", projectId) + "/" + organismNameForFiles + searchDir`
and throws if the result does not exist. `organismNameForFiles` comes from
`apidb.organism.name_for_filenames`, resolved by matching the organism param's internal
value against `sres.TaxonName.name` (:317).

Verified layout of the real variation HSSS files on cedar
(`/home/jbrestel/webserviceTest/Pfalciparum3D7/dnaseq/`):

```
readFreq20/  readFreq40/  readFreq60/  readFreq80/
```

and inside each: 538 numbered strain directories plus `contigIdToSourceId.dat`,
`strainIdToName.dat`, `referenceGenome.dat`. The `readFreq*` level is appended separately
by `FindPolymorphismsAbstractPlugin:106` (`new File(organismDir, "readFreq" + pct)`), and
the four directories match `snpParams.ReadFrequencyPercent`'s enum values (20/40/60/80)
exactly. So `/dnaseq` is the whole of the missing piece.

**Do not touch the chip overrides.** `FindChipPolymorphismsPlugin:42` and
`FindChipSnpMajorAllelesPlugin:72` override `getSearchDir()` to
`/highSpeedChipSnpSearch`. They are equally dead, but leaving them alone keeps this diff
to what it needs to be.

### 3.2 ID separator — `HighSpeedSnpSearch/bin/hsssReconstructSnpId:42-43`

The Perl builds the ID as `$prefix . "$contigSourceId.$location" . $suffix`. Prefix and
suffix are configurable; **the `.` separator is hardcoded**, which is why config alone
cannot produce a variation ID. Change it to `_` in **both** the STDERR and STDOUT `join`s
(the two lines are otherwise identical; the STDOUT one carries the `$seqFilter` guard):

```perl
print STDERR join("\t", $prefix."${contigSourceId}_${location}".$suffix, @fields) . "\n";
print STDOUT join("\t", $prefix."${contigSourceId}_${location}".$suffix, @fields) . "\n"
  unless ($seqFilter && ($contigSourceId ne $seqFilter || $location < $minLoc || $location > $maxLoc));
```

`contigIdToSourceId.dat` maps `1 → Pf3D7_01_v3` (16 contigs for Pf), so with the prefix
from §3.3 the emitted ID becomes `Variant_Pf3D7_01_v3_29514` — exactly the
`VariationRecordClass` `source_id` format.

Braces around the interpolated names are required: `"$prefix$contigSourceId_$location"`
would parse `$contigSourceId_` as a variable name.

### 3.3 ID prefix — a Conifer variable, in `ApiCommonWebsite`

This one is **not** in this repo, and there are three candidate files. Only one is right:

| file | verdict |
|---|---|
| `gus_home/config/<project>/highSpeedSnpSearch-config.xml` | **No** — generated, regenerated on every build, would silently revert |
| `ApiCommonWebService/WSFPlugin/lib/conifer/roles/conifer/templates/ApiCommonWebService/highSpeedSnpSearch-config.xml.j2` | **No** — the template does not hold the value; it renders `{{ highspeedsnpsearchconfig_idPrefix }}` |
| **`ApiCommonWebsite/Model/lib/conifer/roles/conifer/vars/ApiCommon/default.yml:102`** | **Yes** — this is where the value lives |

```yaml
highspeedsnpsearchconfig_idPrefix: Variant_      # was NGS_SNP.
```

**So this change spans two repos.** `ApiCommonWebService` (§3.1, §3.2, §4) and
`ApiCommonWebsite` (this section). Both are on branch `dnaseq-merge-experiments`; the
`ApiCommonWebsite` edit is a one-line var change and needs no build of its own, but the
site must be re-conifered for it to reach `gus_home/config`.

It is an **ApiCommon cohort default**, so it applies to every ApiCommon project —
acceptable only because §2 establishes there are no other consumers. If a second consumer
with a different ID convention ever appears, this must become per-plugin; do not solve that
now.

`HighSpeedSnpSearchAbstractPlugin.getIdPrefix()` (:157-165) reads the property and falls
back to the literal string `"NULL"`, which the Perl treats as empty (`:40-41`).

For **testing only**, the value can be overridden per instance in
`etc/conifer_site_vars.yml` without touching the shared default — the same mechanism §5.1
uses for `webServiceMirror`. Prefer that while iterating; change the cohort default when
the behaviour is settled.

`HighSpeedSnpSearchAbstractPlugin.getIdPrefix()` (:157-165) reads this property and falls
back to the literal string `"NULL"`, which the Perl treats as empty (`:40-41`). The
property is **site-wide across all HSSS plugins** — acceptable only because §2 establishes
there are no other consumers. If a second consumer with a different ID convention ever
appears, this property must become per-plugin; do not solve that now.

### 3.4 Naming: deliberately unchanged

`hsssReconstructSnpId`, the `org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch` package,
and the `Snp`-flavoured class names all stay. Honest naming argues for a rename, but it
touches ten Java files, the installed `gus_home/bin` script names, and every
`processName` in model XML — and bundling a rename with a behavioural fix makes the diff
unreviewable. Worth a follow-up issue; not this change.

## 4. Test fixture updates (part of this change)

`ApiCommonWebService/Test` has a working JUnit harness
(`FindPolymorphismsSearchTest`, `FindMajorAllelesSearchTest`,
`FindPolymorphismsWithSeqFilterSearchTest`, extending `HsssTest`) with a testing seam —
`setOrganismNameForFiles(...)` bypasses the database lookup. **These tests will fail after
§3 unless updated in the same change.**

### 4.1 Rename the fixture directory

```
HighSpeedSnpSearch/test/TestDB/Hsapiens123/highSpeedSnpSearch/  →  .../dnaseq/
```

(containing `readFreq80/`). The tests point `PARAM_WEBSVCPATH` at
`$PROJECT_HOME/ApiCommonWebService/HighSpeedSnpSearch/test/PROJECT_GOES_HERE`, and the
mock project mapper supplies `TestDB` for the `PROJECT_GOES_HERE` substitution.

### 4.2 Update the three expected files with baked-in IDs

In `HighSpeedSnpSearch/test/expected/`:

| file | rows | change |
|---|---|---|
| `genomicLocationFilter.txt` | 2 | `NGS_SNP.e99.2011` → `Variant_e99_2011`; `NGS_SNP.h103.30021` → `Variant_h103_30021` |
| `polymorphismSearchWithSourceIds.txt` | 8 | `a80.896`, `b86.13441`, `e99.2011`, `f100.23`, `g102.4334`, `h103.30021`, `i104.3002`, `j201.54` — each `NGS_SNP.<c>.<l>` → `Variant_<c>_<l>` |
| `polymorphismSearchWithSourceIdsAndSeqFilter.txt` | 1 | `NGS_SNP.f100.23` → `Variant_f100_23` |

All three are tab-delimited, with the ID in column 1 and the remaining columns unchanged.
The transformation is mechanical and total: every `NGS_SNP.` becomes `Variant_`, and the
**single remaining dot** in each ID becomes an underscore. Note the contig names
themselves contain no dots, so a naive global dot-to-underscore replacement is safe here —
but write the change deliberately rather than relying on that.

The other five expected files (`polymorphismSearch.txt`, `majorAlleles.txt`,
`mergeStrains*.txt`) hold pre-reconstruction output — contig **index** and location as
separate columns — and must **not** be touched. Only files whose first column is a
reconstructed ID change.

## 5. Verification

Cheapest rung first. Rung 1 alone proves the entire ID fix and needs no build.

1. **The Perl, directly.** Against the real Pf mapping file on cedar:

```bash
printf '1\t29514\t100\t5\t1\n' \
  | $GUS_HOME/bin/hsssReconstructSnpId \
      /home/jbrestel/webserviceTest/Pfalciparum3D7/dnaseq/readFreq20/contigIdToSourceId.dat \
      1 Variant_ NULL
```

Expect exactly `Variant_Pf3D7_01_v3_29514	100	5	syn`. (Field 5 = `1` maps to `syn` per
the script's coding-class translation; `100`/`5` pass through as knowns/non-major
percentages.) Before the §3.2 change the same command yields
`Variant_Pf3D7_01_v3.29514` — the dot is the bug, so a pre-change run is expected to show it.

2. **Confirm the ID resolves to a real record**, closing the loop the search would:

```sql
SELECT source_id FROM apidbtuning.VariationAttributes
WHERE source_id = 'Variant_Pf3D7_01_v3_29514';
```

One row. This is the check that the whole spec exists to satisfy.

3. **JUnit**, after the §4 updates. Expect the previously-passing HSSS tests to pass
   again — a fixture or expected-file miss shows up here, not in production.

4. **Integration on cedar.** The plugin resolves the real directory. Requires the
   `webServiceMirror` override below.

5. **End-to-end through a real search** — deferred to the `VariationsByIsolateGroup` spec,
   which is the first thing able to exercise it.

### 5.1 Pointing the plugin at the test files

Override `webServiceMirror` in the instance's `etc/conifer_site_vars.yml` — declared
per-instance config, regenerated on build, never committed model XML. Do **not** hardcode a
path in model XML.

The `WebServicesPath` param's internal value is
`@WEBSERVICEMIRROR@/PROJECT_GOES_HERE/build-<N>`, so with
`webServiceMirror: /home/jbrestel/webserviceTest` the plugin looks for
`/home/jbrestel/webserviceTest/PlasmoDB/build-70/Pfalciparum3D7/dnaseq` — two levels
deeper than where the test files actually sit. Bridge it with a symlink:

```bash
mkdir -p /home/jbrestel/webserviceTest/PlasmoDB/build-70
ln -s /home/jbrestel/webserviceTest/Pfalciparum3D7 \
      /home/jbrestel/webserviceTest/PlasmoDB/build-70/Pfalciparum3D7
```

Without it the failure is `"Organism dir does not exist"`, which reads like a code bug
rather than a path problem.

## 6. Facts established for the consuming spec

Verified here so the search spec does not need to re-derive them:

- **Sample names match.** All **216** distinct `sample_stable_id`s in
  `eda.attributevalue_s3be28bbe14_sample` are a strict subset of the **538** strain names
  in `strainIdToName.dat`. So a filterParam whose internal values are EDA sample stable
  IDs will only ever name strains HSSS knows. No mapping layer is needed.
- **The results-file contract is exactly 4 tab-separated columns** —
  `FindPolymorphismsAbstractPlugin:141` throws otherwise. Order:
  `sourceId`, `percentOfKnowns`, `percentOfPolymorphisms`, `phenotype`. `wsColumn`
  declarations must follow it.
- **The organism param's internal value must remain the taxon name**
  (e.g. `Plasmodium falciparum 3D7`), because `getOrganismNameForFiles` looks it up in
  `sres.TaxonName.name`. It cannot be repurposed to carry an EDA study abbreviation; the
  search spec needs a separate hidden param for that.
- **`name_for_filenames`** for the three organisms with dnaseq isolate data:
  `Pfalciparum3D7` (PlasmoDB), `TbruceiTREU927` (TriTrypDB), `AfumigatusAf293` (FungiDB).

## 7. Out of scope

- **`VariationsByIsolateGroup`** and the other three HSSS searches — separate specs in
  `ApiCommonModel`. This spec is a prerequisite for all of them.
- **Renaming** the snp-flavoured classes, script, and package — §3.4.
- **Making `idPrefix` per-plugin** — §3.3. Only needed if a second consumer appears.
- **Removing the dead snp/chip plugins** (`FindChip*`, `FindMajorAlleles*`,
  `FindGenesWithSnpChars*`) and their tests. They are unreferenced, but deleting them is a
  separate decision with its own review.
- **Deleting the orphaned `GenesByNgsSnps`/`GenesBySnps` ontology rows** — §2.
- **Populating the production HSSS directories** under
  `/var/www/Common/apiSiteFilesMirror/webServices/<project>/build-<N>/`. This spec is
  verified against the test copy; production placement is a data-deployment task.
