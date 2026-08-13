# HSSS Variation Plumbing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the HighSpeedSnpSearch (HSSS) plugins emit `variation` record IDs and read
the variation HSSS directory layout, so the ported HSSS searches can work at all.

**Architecture:** Four small edits across two repos — one Perl line (the ID separator), two
Java one-liners (the search directory and the strain-filter param name), and one Conifer
variable (the ID prefix). Plus fixture hygiene. No new files, no new classes: the plugins
are variation-only from here, so they are edited in place.

**Tech Stack:** Java (WSF plugins, built with `bld`), Perl (the `hsss*` scripts installed
into `$GUS_HOME/bin`), Conifer/Ansible (site config generation), `bld` and
`bin/veup-build.sh` from the `agentic-veupath-dev` control plane.

**Spec:** `docs/superpowers/specs/2026-08-05-hsss-variation-plumbing-design.md`. Section
references below prefixed `spec §` point there.

---

## Read this before starting

**There is no test suite to lean on.** Both HSSS harnesses are already broken,
independently of this change (spec §4): the JUnit module references a constant
(`FindPolymorphismsPlugin.PARAM_STRAIN_LIST`) that exists nowhere and therefore does not
compile, and `hsssTestSuite` passes the wrong number of positional args to
`hsssGeneratePolymorphismScript`. **Do not attempt to run either, and do not report a green
test run.** Reviving them is explicitly out of scope.

What we *do* have is one genuinely runnable check that proves the load-bearing half of this
change, needs no build, no database, and no webserver — Task 1 is built around it.

**Two of the four edits cannot be verified until the follow-on search exists** (Task 2 and
Task 3). They are correct by inspection against the spec's evidence. Say so plainly rather
than implying they were exercised.

**Repos and branch.** Both repos are already on `dnaseq-merge-experiments`:

| repo | path |
|---|---|
| `ApiCommonWebService` | `~/workspaces/plasmodb/ApiCommonWebService` |
| `ApiCommonWebsite` | `~/workspaces/plasmodb/ApiCommonWebsite` |

Do **not** create a git worktree and do **not** switch branches. `~/workspaces/plasmodb`
is the source of a mutagen sync to the remote webserver; a worktree would not be synced and
the remote build would compile the un-edited files.

- [ ] **Prerequisite: confirm both branches and that sync is up**

```bash
for r in ApiCommonWebService ApiCommonWebsite; do
  printf '%-22s %s\n' "$r" "$(git -C ~/workspaces/plasmodb/$r rev-parse --abbrev-ref HEAD)"
done
cd ~/workspaces/agentic-veupath-dev && bin/veup-sync-up.sh plasmodb
```

Expected: both print `dnaseq-merge-experiments`, and the `plasmodb` row of the roster reads
`up`. **If either prints `main`, stop.**

## File Structure

| file | change |
|---|---|
| `ApiCommonWebService/HighSpeedSnpSearch/bin/hsssReconstructSnpId` | ID separator `.` → `_` (Task 1) |
| `ApiCommonWebService/WSFPlugin/src/main/java/org/apidb/apicomplexa/wsfplugin/highspeedsnpsearch/HighSpeedSnpSearchAbstractPlugin.java` | `getSearchDir()` → `/dnaseq` (Task 2) |
| `ApiCommonWebService/WSFPlugin/src/main/java/org/apidb/apicomplexa/wsfplugin/highspeedsnpsearch/FindPolymorphismsPlugin.java` | `getStrainFilterParamName()` → `variation_sample_meta` (Task 3) |
| `ApiCommonWebService/HighSpeedSnpSearch/test/TestDB/Hsapiens123/highSpeedSnpSearch/` | rename to `dnaseq/` (Task 4) |
| `ApiCommonWebService/HighSpeedSnpSearch/test/expected/{genomicLocationFilter,polymorphismSearchWithSourceIds,polymorphismSearchWithSourceIdsAndSeqFilter}.txt` | `NGS_SNP.<c>.<l>` → `Variant_<c>_<l>` (Task 4) |
| `ApiCommonWebsite/Model/lib/conifer/roles/conifer/vars/ApiCommon/default.yml` | `highspeedsnpsearchconfig_idPrefix: Variant_` (Task 5) |

One task per edit, each independently committable. Task 1 first because it is the only one
with a real test, and Task 6 deploys and re-verifies.

---

### Task 1: Fix the ID separator

This is the change the whole spec exists for. `hsssReconstructSnpId` builds
`$prefix . "$contigSourceId.$location" . $suffix`; prefix and suffix are configurable but
the `.` is hardcoded, so no amount of config can produce a `Variant_<seq>_<loc>` ID.

**Files:**
- Modify: `ApiCommonWebService/HighSpeedSnpSearch/bin/hsssReconstructSnpId:42-43`

- [ ] **Step 1: Run the failing test and record its output**

The script needs only `strict`, so it runs straight from the source tree with the
checked-in fixture — no build, no database, no webserver:

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService/HighSpeedSnpSearch && \
printf '80\t896\t100\t25\t1\n' \
  | perl bin/hsssReconstructSnpId test/textData/contigIdToSourceId.dat 1 Variant_ NULL 2>/dev/null
```

Expected **now** (this is the bug):

```
Variant_a80.896	100	25	syn
```

Note the `.` between `a80` and `896`. The fixture maps contig index `80 → a80`; index `1`
does **not** exist in it and would make the script die with
`Can't map contigIndex '1' in stdin`, so use `80`. The trailing `1` in the input is the
coding class, which the script translates to `syn`.

- [ ] **Step 2: Make the change**

In `bin/hsssReconstructSnpId`, replace lines 42-43:

```perl
  print STDERR join("\t", $prefix."$contigSourceId.$location".$suffix, @fields) . "\n" ;
  print STDOUT join("\t", $prefix."$contigSourceId.$location".$suffix, @fields) . "\n"  unless ($seqFilter && ($contigSourceId ne $seqFilter || $location  < $minLoc || $location > $maxLoc));
```

with:

```perl
  print STDERR join("\t", $prefix."${contigSourceId}_${location}".$suffix, @fields) . "\n" ;
  print STDOUT join("\t", $prefix."${contigSourceId}_${location}".$suffix, @fields) . "\n"  unless ($seqFilter && ($contigSourceId ne $seqFilter || $location  < $minLoc || $location > $maxLoc));
```

**Both** lines change — STDERR and STDOUT emit the same ID and must stay consistent.

The `${...}` braces are required, not stylistic: `"$prefix$contigSourceId_$location"` would
make Perl look for a variable named `$contigSourceId_`, which is undefined, silently
producing `Variant_896`.

- [ ] **Step 3: Run the test again**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService/HighSpeedSnpSearch && \
printf '80\t896\t100\t25\t1\n' \
  | perl bin/hsssReconstructSnpId test/textData/contigIdToSourceId.dat 1 Variant_ NULL 2>/dev/null
```

Expected:

```
Variant_a80_896	100	25	syn
```

- [ ] **Step 4: Confirm the shape matches a real variation ID**

The synthetic fixture proves the format; this confirms the format is the *right* one.
Requires the ssh tunnel to genomicsdb on port 5432:

```bash
psql -h localhost -p 5432 -d unidb_shu_a -tAc \
  "SELECT source_id FROM apidbtuning.VariationAttributes WHERE source_id = 'Variant_Pf3D7_01_v3_29514'"
```

Expected: `Variant_Pf3D7_01_v3_29514` (one row). That is `Variant_` + `Pf3D7_01_v3` + `_` +
`29514` — the exact composition Step 3 now produces. If this returns nothing, stop: either
the tunnel is down or the record's ID convention is not what the spec assumed.

- [ ] **Step 5: Check no other script builds IDs the same way**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService/HighSpeedSnpSearch/bin && \
  grep -n 'contigSourceId\.\$location\|contigSourceId\.\$loc' * ; echo "exit: $?"
```

Expected: `exit: 1` (no matches) — `hsssReconstructChipSnpId` is for the dead chip path and
composes IDs differently; if this *does* match something, report it rather than changing it,
since chip is out of scope (spec §7).

> **This expectation was wrong, and the step earned its keep by catching it.** The grep
> matches `hsssGenomicLocationsFilter:51` and `:67`, a second ID-composition site on a
> *live* alternative pipeline tail. Handled by Task 1b — do not change it here.

- [ ] **Step 6: Commit**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService
git add HighSpeedSnpSearch/bin/hsssReconstructSnpId
git commit -m "Build variation IDs with an underscore separator

hsssReconstructSnpId hardcoded a '.' between sequence and location, so no
combination of the configurable idPrefix/idSuffix could produce a
VariationRecordClass source_id (Variant_Pf3D7_01_v3_29514). Both the STDOUT
and STDERR joins now use '_'.

Verified against the checked-in fixture: contig 80/location 896 with prefix
Variant_ now yields Variant_a80_896, and the resulting shape matches a real
row in apidbtuning.VariationAttributes.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 1b: Fix the separator in the second ID-composition site

**Added during execution.** Task 1's Step 5 was written expecting no other script to compose
IDs the same way. It does: `hsssGenomicLocationsFilter` builds
`$idPrefix."$contigSourceId.$location".$idSuffix` at **two** places, and it is not dead code
on the chip path — it is a live alternative tail of the *same* pipeline.

Why it matters, concretely. `HsssGenomicLocationFilterScriptGenerator.pm:14` returns this
script from `getFinalCommandString`, so it **substitutes for** `hsssReconstructSnpId` rather
than running after it. Tracing which planned search reaches which tail:

| plugin | generate script | ID composed by | fixed by |
|---|---|---|---|
| `FindPolymorphismsPlugin` → `VariationsByIsolateGroup` | `hsssGeneratePolymorphismScript` (inherited) | `hsssReconstructSnpId` | Task 1 |
| `FindPolymorphismsWithSeqFilterPlugin` → `VariationsByLocation` | inherited, not overridden | `hsssReconstructSnpId` | Task 1 |
| **`FindSnpsByGeneIdsPlugin`** → **`VariationsByGeneIds`** | **overrides** (`:112`) to `hsssGenerateGenomicLocationsScript` | **`hsssGenomicLocationsFilter`** | **this task** |

So exactly one of the four searches being ported would still emit
`Variant_Pf3D7_01_v3.29514` and fail the way this change exists to prevent — silently, with
zero results. Fixing it now costs the same two lines; deferring it buys a future debugging
session.

**Files:**
- Modify: `ApiCommonWebService/HighSpeedSnpSearch/bin/hsssGenomicLocationsFilter:51` and `:67`

- [ ] **Step 1: Confirm both sites and their context**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService/HighSpeedSnpSearch/bin && \
  grep -n 'contigSourceId\.\$location' hsssGenomicLocationsFilter
```

Expected: two hits, lines 51 and 67. They are the same statement in two branches of the
filter's control flow — the "within current filter" branch and the "within the next filter"
branch — so both must change or gene-ID searches would emit inconsistent IDs depending on
which branch a given variant took.

- [ ] **Step 2: Make the change at both sites**

Replace, at both line 51 and line 67:

```perl
    print STDOUT join("\t", $idPrefix."$contigSourceId.$location".$idSuffix, @fields) . "\n";
```

with:

```perl
    print STDOUT join("\t", $idPrefix."${contigSourceId}_${location}".$idSuffix, @fields) . "\n";
```

Note the indentation differs between the two sites (line 67 sits one level deeper inside the
`while`/`if`). Preserve each line's existing leading whitespace; change only the
interpolation. The `${...}` braces are required for the same reason as Task 1 —
`$contigSourceId_` would be read as an undefined variable name.

- [ ] **Step 3: Verify no dotted composition remains anywhere in `bin/`**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService/HighSpeedSnpSearch/bin && \
  grep -n 'contigSourceId\.\$location\|contigSourceId\.\$loc' * ; echo "exit: $?"
```

Expected: `exit: 1`, no matches — this is now the assertion Task 1's Step 5 was originally
written to make.

- [ ] **Step 4: Confirm the underscore form is present twice**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService/HighSpeedSnpSearch/bin && \
  grep -cF '${contigSourceId}_${location}' hsssGenomicLocationsFilter hsssReconstructSnpId
```

Expected: `hsssGenomicLocationsFilter:2` and `hsssReconstructSnpId:2`.

> **`-F` is required.** Without it `grep` treats the pattern as a basic regex, where `{`/`}`
> are interval syntax, and it matches **nothing** — reporting `0` even for correct code. An
> earlier draft of this step omitted the flag, which made it a check that could only fail;
> the risk is an implementer "fixing" working code to satisfy it. Use `-F` for any grep of a
> literal Perl interpolation.

- [ ] **Step 5: Check the script is still syntactically valid**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService/HighSpeedSnpSearch/bin && \
  perl -c hsssGenomicLocationsFilter
```

Expected: `hsssGenomicLocationsFilter syntax OK`. There is no fixture-driven test for this
script the way there is for `hsssReconstructSnpId` (`hsssTestSuite`, the only caller with
fixture data, is broken — spec §4), so a syntax check plus the greps is the available
verification. Do not claim more.

- [ ] **Step 6: Commit**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService
git add HighSpeedSnpSearch/bin/hsssGenomicLocationsFilter
git commit -m "Build variation IDs with an underscore in the locations filter too

hsssGenomicLocationsFilter composes source_ids the same dotted way
hsssReconstructSnpId did, at both of its output branches. It is not dead
chip code: HsssGenomicLocationFilterScriptGenerator returns it as the final
command, so it substitutes for the reconstruct script rather than following
it, and FindSnpsByGeneIdsPlugin overrides getGenerateScriptName to route
through it.

Without this, VariationsByGeneIds would still emit dotted IDs matching no
variation record -- zero results, no error -- while the isolate-group and
location searches worked, since those inherit the reconstruct path.

Found by Task 1's Step 5 grep, which was written expecting no second site.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 2: Point the plugin at the variation directory

`findOrganismDir` composes
`webSvcPath.replaceAll("PROJECT_GOES_HERE", projectId) + "/" + organismNameForFiles + searchDir`
and throws if the result is absent. The variation HSSS files live under `dnaseq`, not
`highSpeedSnpSearch`.

**Files:**
- Modify: `ApiCommonWebService/WSFPlugin/src/main/java/org/apidb/apicomplexa/wsfplugin/highspeedsnpsearch/HighSpeedSnpSearchAbstractPlugin.java:196-198`

- [ ] **Step 1: Confirm the real layout on the webserver**

```bash
cd ~/workspaces/agentic-veupath-dev && \
  ssh -o LogLevel=ERROR "$(python3 bin/resolve.py --profile profiles/plasmodb.yml --field host)" \
  'ls /home/jbrestel/webserviceTest/Pfalciparum3D7/dnaseq/'
```

Expected: `readFreq20  readFreq40  readFreq60  readFreq80`. The `readFreq*` level is
appended separately by `FindPolymorphismsAbstractPlugin:106`, so `/dnaseq` is exactly the
piece `getSearchDir()` must supply. (Harmless ssh port-forward warnings may appear on
stderr; ignore them.)

- [ ] **Step 2: Make the change**

In `HighSpeedSnpSearchAbstractPlugin.java`, replace:

```java
    protected String getSearchDir() {
        return  "/highSpeedSnpSearch";
    }
```

with:

```java
    protected String getSearchDir() {
        return  "/dnaseq";
    }
```

Preserve the file's existing odd indentation and the double space after `return` — this
file is inconsistently formatted and reflowing it would bury a one-word change in
whitespace noise.

- [ ] **Step 3: Verify the edit, and that the chip overrides are untouched**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService/WSFPlugin/src/main/java/org/apidb/apicomplexa/wsfplugin/highspeedsnpsearch && \
  grep -n 'getSearchDir' -A 2 HighSpeedSnpSearchAbstractPlugin.java FindChipPolymorphismsPlugin.java FindChipSnpMajorAllelesPlugin.java
```

Expected: the abstract plugin returns `/dnaseq`; **both** chip plugins still return
`/highSpeedChipSnpSearch`. The chip path is equally dead but out of scope (spec §7), and
leaving it alone keeps the diff honest.

- [ ] **Step 4: Note that this is unverifiable until the search exists**

There is no way to exercise `findOrganismDir` without a search invoking the plugin (spec
§5 rung 5). Do not claim otherwise. Record in the task notes: *"correct by inspection
against the directory listing in Step 1; first exercised by the
`VariationsByIsolateGroup` search."*

- [ ] **Step 5: Commit**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService
git add WSFPlugin/src/main/java/org/apidb/apicomplexa/wsfplugin/highspeedsnpsearch/HighSpeedSnpSearchAbstractPlugin.java
git commit -m "Read HSSS variation data from the dnaseq directory

The variation HSSS files are laid out as <organism>/dnaseq/readFreq<N>/,
not <organism>/highSpeedSnpSearch/readFreq<N>/, so findOrganismDir would
throw 'Organism dir does not exist'. The chip plugins keep their own
override.

Not independently verifiable -- findOrganismDir is only reached when a
search invokes the plugin.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 3: Rename the strain filter param to variation vocabulary

`FindPolymorphismsAbstractPlugin:100` reads the samples selection under whatever name
`getStrainFilterParamName()` returns, and `:41` declares it **required**. So this string is
a contract with the model XML the follow-on spec will write.

**Files:**
- Modify: `ApiCommonWebService/WSFPlugin/src/main/java/org/apidb/apicomplexa/wsfplugin/highspeedsnpsearch/FindPolymorphismsPlugin.java:41-43`

- [ ] **Step 1: Make the change**

Replace:

```java
      protected String getStrainFilterParamName() {
      return "ngsSnp_strain_meta";
  }
```

with:

```java
      protected String getStrainFilterParamName() {
      return "variation_sample_meta";
  }
```

Again, keep the existing (misaligned) indentation.

- [ ] **Step 2: Confirm nothing else references the old name in this repo**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService && \
  grep -rn 'ngsSnp_strain_meta' . ; echo "exit: $?"
```

Expected: `exit: 1`. If `FindChipPolymorphismsPlugin` shows up, check what *it* returns —
it has its own override and must not be changed here.

The old name still exists in `ApiCommonModel/Model/lib/wdk/model/questions/params/snpParams.xml`,
which is dead, commented-out snp XML. That is expected and out of scope; do not edit
`ApiCommonModel` in this plan.

- [ ] **Step 3: Note the contract for the follow-on spec**

Record in the task notes: *"the consuming `filterParam` must be named exactly
`variation_sample_meta`; a mismatch is rejected as a missing required parameter."* Like
Task 2 this has no runtime symptom until a search exists.

- [ ] **Step 4: Commit**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService
git add WSFPlugin/src/main/java/org/apidb/apicomplexa/wsfplugin/highspeedsnpsearch/FindPolymorphismsPlugin.java
git commit -m "Name the strain filter param for variations, not snps

getStrainFilterParamName is a contract with the model XML: the consuming
filterParam must carry this exact name or the plugin rejects the request as
missing a required parameter. Renaming it now, while the plugin has no
consumer, keeps snp vocabulary out of new variation model XML.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 4: Fixture hygiene

**These fixtures are consumed only by the two broken harnesses** (spec §4), so this task
fixes no test and unblocks nothing. It is worth ~15 lines so that whoever revives a harness
starts from fixtures consistent with the current ID convention rather than debugging a
stale one. **Do not run either harness, and do not report a test result from this task.**

**Files:**
- Rename: `ApiCommonWebService/HighSpeedSnpSearch/test/TestDB/Hsapiens123/highSpeedSnpSearch/` → `.../dnaseq/`
- Modify: `ApiCommonWebService/HighSpeedSnpSearch/test/expected/genomicLocationFilter.txt`
- Modify: `ApiCommonWebService/HighSpeedSnpSearch/test/expected/polymorphismSearchWithSourceIds.txt`
- Modify: `ApiCommonWebService/HighSpeedSnpSearch/test/expected/polymorphismSearchWithSourceIdsAndSeqFilter.txt`

- [ ] **Step 1: Rename the fixture directory to match Task 2**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService/HighSpeedSnpSearch/test/TestDB/Hsapiens123 && \
  git mv highSpeedSnpSearch dnaseq && ls dnaseq/
```

Expected: `readFreq80`. Use `git mv` so the rename is tracked rather than showing as a
delete plus an add.

- [ ] **Step 2: Rewrite the IDs in the three expected files**

Every ID is `NGS_SNP.<contig>.<location>` and becomes `Variant_<contig>_<location>`. The
contig names contain no dots, so each ID has exactly one dot to convert:

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService/HighSpeedSnpSearch/test/expected && \
sed -i -E 's/^NGS_SNP\.([^.\t]+)\.([0-9]+)\t/Variant_\1_\2\t/' \
  genomicLocationFilter.txt \
  polymorphismSearchWithSourceIds.txt \
  polymorphismSearchWithSourceIdsAndSeqFilter.txt
```

The anchor `^` and the trailing `\t` confine the substitution to column 1, so the remaining
columns cannot be touched.

- [ ] **Step 3: Verify the rewrite is complete and correct**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService/HighSpeedSnpSearch/test/expected && \
  echo "--- remaining NGS_SNP (want none):" && { grep -c 'NGS_SNP' *.txt || true; } && \
  echo "--- new IDs:" && cut -f1 genomicLocationFilter.txt polymorphismSearchWithSourceIds.txt polymorphismSearchWithSourceIdsAndSeqFilter.txt
```

Expected: every file reports `0` for `NGS_SNP`, and the IDs are exactly these 11 —
`Variant_e99_2011`, `Variant_h103_30021` (from `genomicLocationFilter.txt`);
`Variant_a80_896`, `Variant_b86_13441`, `Variant_e99_2011`, `Variant_f100_23`,
`Variant_g102_4334`, `Variant_h103_30021`, `Variant_i104_3002`, `Variant_j201_54` (from
`polymorphismSearchWithSourceIds.txt`); `Variant_f100_23` (from the SeqFilter file).

- [ ] **Step 4: Confirm the untouched fixtures stayed untouched**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService && git status --short HighSpeedSnpSearch/test/
```

Expected: exactly the three `expected/*.txt` modifications plus the renamed directory.
`polymorphismSearch.txt`, `majorAlleles.txt`, and the `mergeStrains*.txt` files hold
pre-reconstruction output (contig **index** and location as separate columns) and must not
appear.

- [ ] **Step 5: Commit**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService
git add HighSpeedSnpSearch/test/
git commit -m "Update HSSS fixtures to the variation ID convention

Renames the fixture search dir to dnaseq and rewrites the 11 baked-in IDs in
the three expected files from NGS_SNP.<contig>.<loc> to
Variant_<contig>_<loc>.

Fixes no test: both HSSS harnesses are already broken independently of this
change -- the JUnit module references a constant that exists nowhere, and
hsssTestSuite passes the wrong argument count to
hsssGeneratePolymorphismScript. This only means a future revival starts from
fixtures matching the current convention.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 5: Set the ID prefix (different repo)

The `idPrefix` is **not** in `ApiCommonWebService`. The generated
`gus_home/config/<project>/highSpeedSnpSearch-config.xml` is regenerated on every build,
and the `.j2` template only renders `{{ highspeedsnpsearchconfig_idPrefix }}`. The value
lives in `ApiCommonWebsite`.

**Files:**
- Modify: `ApiCommonWebsite/Model/lib/conifer/roles/conifer/vars/ApiCommon/default.yml:102`

- [ ] **Step 1: Confirm the current value and that it is the only definition**

```bash
cd ~/workspaces/plasmodb && grep -rn 'highspeedsnpsearchconfig_idPrefix' \
  --include=*.yml --include=*.yaml --include=*.j2 .
```

Expected exactly two hits: the template in
`ApiCommonWebService/WSFPlugin/lib/conifer/roles/conifer/templates/ApiCommonWebService/highSpeedSnpSearch-config.xml.j2`
(which consumes it) and
`ApiCommonWebsite/Model/lib/conifer/roles/conifer/vars/ApiCommon/default.yml:102`
(which defines it, as `NGS_SNP.`).

- [ ] **Step 2: Make the change**

In `ApiCommonWebsite/Model/lib/conifer/roles/conifer/vars/ApiCommon/default.yml`, replace:

```yaml
highspeedsnpsearchconfig_idPrefix: NGS_SNP.
```

with:

```yaml
highspeedsnpsearchconfig_idPrefix: Variant_
```

Leave `highspeedsnpsearchconfig_jobsDir` (the line above) and the
`highspeedchipsnpsearchconfig_*` block (below) alone.

- [ ] **Step 3: Verify**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebsite && \
  grep -n 'highspeedsnpsearchconfig_' Model/lib/conifer/roles/conifer/vars/ApiCommon/default.yml && \
  git diff --stat
```

Expected: `idPrefix: Variant_`, `jobsDir` unchanged, and `1 file changed, 1 insertion(+), 1 deletion(-)`.

- [ ] **Step 4: Commit (in `ApiCommonWebsite`, not `ApiCommonWebService`)**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebsite
git add Model/lib/conifer/roles/conifer/vars/ApiCommon/default.yml
git commit -m "Prefix HSSS result IDs with Variant_ instead of NGS_SNP.

Paired with the separator fix in ApiCommonWebService, this makes the HSSS
plugins emit VariationRecordClass source_ids. This is an ApiCommon cohort
default and so applies to every project in the cohort, which is safe because
no project has a live snp or chip search -- those imports are commented out
in the shared apiCommonModel.xml.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 6: Build, deploy, and re-verify the installed copy

Task 1 proved the fix in the **source tree**. The plugins run the copy installed in
`$GUS_HOME/bin`, and the prefix comes from the **generated** config — neither of which
exists yet.

**Files:** none (build and deploy only).

- [ ] **Step 1: Build and install `ApiCommonWebService`**

```bash
cd ~/workspaces/agentic-veupath-dev && \
  ssh -o LogLevel=ERROR "$(python3 bin/resolve.py --profile profiles/plasmodb.yml --field host)" \
  "bash -lc 'source /var/www/jbrestel.plasmodb.org/etc/setenv && bld ApiCommonWebService'"
```

Expected: `BUILD SUCCESSFUL`. This installs both the Java (WSFPlugin) and the Perl
(HighSpeedSnpSearch) components. Get the docroot for the `setenv` path with
`python3 bin/resolve.py --profile profiles/plasmodb.yml --field docroot` if the user prefix
is not `jbrestel`.

Note `Test-Installation` is **not** in `build.xml`'s default depends list, so the
non-compiling JUnit module is not built. That is why this succeeds despite spec §4.

- [ ] **Step 2: Re-run Task 1's test against the *installed* script**

```bash
cd ~/workspaces/agentic-veupath-dev && \
  ssh -o LogLevel=ERROR "$(python3 bin/resolve.py --profile profiles/plasmodb.yml --field host)" \
  "bash -lc 'source /var/www/jbrestel.plasmodb.org/etc/setenv && \
   printf \"80\t896\t100\t25\t1\n\" | \$GUS_HOME/bin/hsssReconstructSnpId \
     \$PROJECT_HOME/ApiCommonWebService/HighSpeedSnpSearch/test/textData/contigIdToSourceId.dat \
     1 Variant_ NULL 2>/dev/null'"
```

Expected: `Variant_a80_896	100	25	syn`. A dot here means the install did not pick up the
edit — check that mutagen synced the file before Step 1 ran.

- [ ] **Step 3: Regenerate the site config so the new prefix lands**

```bash
cd ~/workspaces/agentic-veupath-dev && \
  ssh -o LogLevel=ERROR "$(python3 bin/resolve.py --profile profiles/plasmodb.yml --field host)" \
  "bash -lc 'source /var/www/jbrestel.plasmodb.org/etc/setenv && \
   conifer configure --cohort ApiCommon --project PlasmoDB \
     --webapp-ctx plasmo.jbrestel --tomcat-webapp-ctx plasmo.jbrestel \
     --site-vars /var/www/jbrestel.plasmodb.org/etc/conifer_site_vars.yml'"
```

Expected: `PLAY RECAP ... failed=0`, with `highSpeedSnpSearch-config.xml` listed as `changed`
(or `ok` if already correct — the playbook is idempotent).

**All four flags are required, and two were missing from an earlier draft of this step.**
`--cohort` is rejected outright (`conifer: error: --cohort is required for configure`).
Omitting `--tomcat-webapp-ctx` gets *further* — it regenerates most files, including our
target — and then fails on `log4j2.json` with
`AnsibleUndefinedVariable: 'tomcat_webapp_ctx' is undefined`, leaving `failed=1`. That is the
worst kind of half-success: the thing you were checking for *did* land, so a careless reading
calls it done while one config file silently went unregenerated. Always read the `PLAY RECAP`.

Both `*-ctx` values are the webapp context, `plasmo.jbrestel` — confirmed against
`/usr/local/tomcat_instances/PlasmoDB/conf/Catalina/localhost/`, which holds one `.xml` per
context. For a different developer prefix, substitute accordingly.

**`conifer install` is not needed as a separate step.** An earlier draft called for it.
`bld ApiCommonWebService` depends on `ApiCommonWebsite-Installation`, which already copies the
edited vars into `gus_home/lib/conifer/roles/conifer/vars/ApiCommon/default.yml` — verify
with `grep highspeedsnpsearchconfig_idPrefix` on that path if `configure` seems to render a
stale value.

Conifer regenerates `model.prop` among other files, so confirm the model still loads
afterwards:

```bash
cd ~/workspaces/agentic-veupath-dev && \
  ssh -o LogLevel=ERROR "$(python3 bin/resolve.py --profile profiles/plasmodb.yml --field host)" \
  "bash -lc 'source /var/www/jbrestel.plasmodb.org/etc/setenv && wdkXml -model PlasmoDB 2>&1 | tail -3'"
```

Expected: ends with `WDK Model resources released.` (a clean shutdown, i.e. the model loaded).

Then reload so the runtime picks up the new config — WSF plugins read their property file at
init, so the prefix does not take effect until this:

```bash
cd ~/workspaces/agentic-veupath-dev && bin/veup-build.sh plasmodb reload
```

Expected: `OK - Reloaded application at context path [/plasmo.jbrestel]`.

- [ ] **Step 4: Confirm the prefix reached the generated config**

```bash
cd ~/workspaces/agentic-veupath-dev && \
  ssh -o LogLevel=ERROR "$(python3 bin/resolve.py --profile profiles/plasmodb.yml --field host)" \
  'grep idPrefix /var/www/jbrestel.plasmodb.org/gus_home/config/highSpeedSnpSearch-config.xml'
```

Expected: `<entry key="idPrefix">Variant_</entry>`. This is the **only** check that Task 5
took effect; nothing else reads that file until a search runs.

> **Note the path.** This file sits directly in `gus_home/config/`, **not** in the
> per-project `gus_home/config/PlasmoDB/` subdirectory where `model.prop` and
> `model-config.xml` live. An earlier draft of this step had the `PlasmoDB/` path and would
> have failed with `No such file or directory` — which reads like "Task 5 didn't work" rather
> than "the path is wrong."

- [ ] **Step 5: Record what remains unverified**

Note explicitly in the task notes: the search directory (Task 2) and the filter param name
(Task 3) are **not exercised by anything in this plan**. Both are first tested by the
`VariationsByIsolateGroup` search. Do not describe this change as end-to-end verified.

---

### Task 7: Close out the spec

**Files:**
- Modify: `ApiCommonWebService/docs/superpowers/specs/2026-08-05-hsss-variation-plumbing-design.md` (the `Status:` line)
- Modify: `ApiCommonWebService/docs/superpowers/plans/2026-08-05-hsss-variation-plumbing.md` (this file)

- [ ] **Step 1: Confirm the change set across both repos**

```bash
for r in ApiCommonWebService ApiCommonWebsite; do
  echo "=== $r"; git -C ~/workspaces/plasmodb/$r log --oneline origin/dnaseq-merge-experiments..HEAD 2>/dev/null || git -C ~/workspaces/plasmodb/$r log --oneline -5
  git -C ~/workspaces/plasmodb/$r status --porcelain
done
```

Expected: `ApiCommonWebService` has the four commits from Tasks 1-4 (plus the spec commit
`97ba19d`), `ApiCommonWebsite` has the one from Task 5, and both working trees are clean.

- [ ] **Step 2: Mark the spec implemented**

Change the spec's:

```markdown
**Status:** approved
```

to:

```markdown
**Status:** implemented 2026-08-05 — ID fix verified; the search directory (§3.1) and filter param name (§3.5) await the `VariationsByIsolateGroup` search
```

- [ ] **Step 3: Add an execution-outcome section to this plan**

Append a short section recording: the four commit SHAs plus the `ApiCommonWebsite` one,
the actual before/after output of Task 1's test, whether `conifer configure` worked as
written or needed the rebuild fallback, and the explicit statement that Tasks 2 and 3 are
unverified pending the follow-on search.

- [ ] **Step 4: Commit**

```bash
cd ~/workspaces/plasmodb/ApiCommonWebService
git add docs/superpowers/specs/2026-08-05-hsss-variation-plumbing-design.md \
        docs/superpowers/plans/2026-08-05-hsss-variation-plumbing.md
git commit -m "Mark the HSSS variation plumbing spec implemented

The ID fix is verified against both the source and installed scripts and the
prefix reaches the generated config. The search directory and filter param
name are correct by inspection but unexercised until the
VariationsByIsolateGroup search exists.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Execution outcome (2026-08-05)

Executed subagent-per-task. Production code: **14 files, 17 insertions, 17 deletions**, and
`grep -rn jbrestel` over `WSFPlugin/src`, `HighSpeedSnpSearch/bin`, `HighSpeedSnpSearch/lib`
is clean.

| task | commit | repo |
|---|---|---|
| 1 — ID separator | `7d1268a27` | `ApiCommonWebService` |
| 1b — second ID site | `5c80e5f37` | `ApiCommonWebService` |
| 2 — `/dnaseq` search dir | `141602e54` | `ApiCommonWebService` |
| 3 — `variation_sample_meta` | `e46356743` | `ApiCommonWebService` |
| 4 — fixture hygiene | `7f7d970` | `ApiCommonWebService` |
| 5 — `idPrefix: Variant_` | `1da7c420a` | **`ApiCommonWebsite`** |

### Verified

- **The ID fix, twice.** Source tree and installed copy both emit `Variant_a80_896` where
  they previously emitted `Variant_a80.896`, from
  `printf '80\t896\t100\t25\t1\n' | hsssReconstructSnpId .../contigIdToSourceId.dat 1 Variant_ NULL`.
- **The format is the right one:** `Variant_Pf3D7_01_v3_29514` exists in
  `apidbtuning.VariationAttributes`.
- `bld ApiCommonWebService` → `BUILD SUCCESSFUL`, 1m44s.
- `conifer configure` → `failed=0`; the generated
  `gus_home/config/highSpeedSnpSearch-config.xml` now reads
  `<entry key="idPrefix">Variant_</entry>`.
- `wdkXml -model PlasmoDB` still loads cleanly after Conifer regenerated `model.prop`, and
  `WEBSERVICEMIRROR`/`PROJECT_ID` are unchanged.
- Webapp reloaded (`OK - Reloaded application at context path [/plasmo.jbrestel]`); the three
  error logs were last written days-to-months ago, i.e. nothing was appended by any of this.

### Not verified, and cannot be from this plan

**The search directory (Task 2) and the filter param name (Task 3) were never exercised.**
Nothing invokes the plugin until a variation search exists. Both are correct by inspection —
Task 2 against the real `dnaseq/readFreq{20,40,60,80}` listing, Task 3 against
`FindPolymorphismsAbstractPlugin:41,100` — and both are first tested by
`VariationsByIsolateGroup`. This change is **not** end-to-end verified.

No automated test suite ran, by design: both HSSS harnesses are broken independently of this
work (§4).

### What execution changed about the plan

Four errors, three in the plan and one in the spec, all found by executing rather than
reviewing:

1. **A whole extra task.** Task 1's Step 5 grep asserted no other script composed IDs the
   dotted way. It found `hsssGenomicLocationsFilter:51,67` — a *live* alternative pipeline
   tail that `FindSnpsByGeneIdsPlugin:112` routes through, so `VariationsByGeneIds` would have
   shipped returning **zero results with no error** while the other searches worked. Became
   Task 1b. The step earned its keep by being wrong.
2. **A check that could only fail.** Task 1b's Step 4 used `grep -c` without `-F` on a pattern
   containing `${...}`; BRE treats the braces as interval syntax and matches nothing, so it
   reported `0` even for already-correct code. The hazard is an implementer "fixing" working
   code to satisfy it.
3. **A wrong config path.** `highSpeedSnpSearch-config.xml` lives in `gus_home/config/`, not
   `gus_home/config/PlasmoDB/`. The original grep would have failed with
   `No such file or directory`, reading as "Task 5 didn't work."
4. **An incomplete `conifer` invocation.** It needs `--cohort`, `--project`, `--webapp-ctx`
   **and** `--tomcat-webapp-ctx`. Missing `--cohort` is refused outright; missing
   `--tomcat-webapp-ctx` regenerates most files — *including the one being checked* — then
   fails on `log4j2.json`. A half-success where the verification passes while a config
   silently goes unregenerated. Read the `PLAY RECAP`.

Also: `conifer install` is unnecessary — `bld ApiCommonWebService` depends on
`ApiCommonWebsite-Installation`, which installs the Conifer vars.

### Findings deferred rather than acted on

Three second sites were found and consciously left alone; each is recorded with reasoning in
"Deliberately not in this plan" below: `FindMajorAllelesPlugin`'s `ngsSnp_strain_meta_a`/`_m`
param family (deferred to the two-isolate-groups spec, **must not be forgotten there**),
`hsssCopyFilesToWebSvcDir`'s write path, the three-way duplication of the ID format, and the
unconditional stderr echo in `hsssReconstructSnpId:42`.

## Deliberately not in this plan

- **Any WDK model XML.** `VariationsByIsolateGroup` is a separate spec in `ApiCommonModel`
  and is the first consumer of everything here.
- **Reviving either HSSS test harness** — spec §4. The JUnit module does not compile and
  the shell suite passes the wrong argument count. Both are larger jobs than this change.
- **Renaming** `hsssReconstructSnpId`, the `highspeedsnpsearch` package, or the
  `Snp`-flavoured class names — spec §3.4. Ten files plus installed script names; bundling
  a rename with a behavioural fix makes the diff unreviewable.
- **Deleting the dead chip/major-alleles/gene-chars plugins** — spec §7.
- **Making `idPrefix` per-plugin.** Only needed if a second consumer with a different ID
  convention appears.
- **Renaming `FindMajorAllelesPlugin`'s param constants.** `:20` and `:24` hardcode
  `ngsSnp_strain_meta_a` and `ngsSnp_strain_meta_m`, with matching `_wiz` variants in
  `ApiCommonModel`'s `sharedParams.xml` — a family of four snp-vocabulary names. That plugin
  extends `HighSpeedSnpSearchAbstractPlugin` directly, so it has no
  `getStrainFilterParamName()` to override; the constants are its own contract, listed in its
  `getRequiredParameterNames()` (`:48-49`) and read at `:81` and `:97`. Same failure mode as
  Task 3: a name mismatch is rejected as a missing required parameter.

  Flagged during Task 3 and **deliberately deferred** to the spec for
  `VariationsByTwoIsolateGroups`, the search it serves (`NgsSnpsByTwoIsolateGroups` and
  `...Wiz`). Reasoning: that search needs per-strain data the variation pipeline does not yet
  have, so it is the furthest out of the four ports, and renaming now would mean choosing a
  param-family name before designing the search that uses it. Note the asymmetric `_a`/`_m`
  suffixes — the prompts read "Set A Isolates" / "Set B Isolates", so `_m` appears to be a
  typo for `_b`, and that should be decided deliberately rather than mirrored.

  > **Whoever writes that spec must include this rename**, or the new variation XML inherits
  > snp naming permanently.

- **Collapsing the ID-composition sites into one helper.** After Task 1b the separator is
  hardcoded in three places (`hsssReconstructSnpId:42-43` and
  `hsssGenomicLocationsFilter:51,67`), all of them re-deriving a format that
  `VariationRecordClass` owns. That duplication is how the second site got missed in the
  first place, so a shared helper is genuinely the right long-term shape — but it is a
  refactor of live pipeline code with no test harness to catch a mistake, which is a worse
  bet right now than three verified one-liners. Worth a follow-up issue.
- **The unconditional STDERR echo in `hsssReconstructSnpId:42`.** Every composed ID is
  printed to stderr as well as stdout, and the stderr copy **bypasses** the stdout branch's
  sequence/location filter — so it emits rows the search deliberately excluded. Pre-existing
  and untouched here, but at 4.4M variants it will make remote logs noisy and could mislead
  anyone reading them during debugging. Flagged during Task 1b; not fixed because changing
  output streams in untested pipeline code is its own change.
- **Populating the production HSSS directories** under
  `/var/www/Common/apiSiteFilesMirror/webServices/<project>/build-<N>/`. This change is
  verified against the test copy in `/home/jbrestel/webserviceTest`; production placement is
  a data-deployment task.
- **`HighSpeedSnpSearch/bin/hsssCopyFilesToWebSvcDir:36`**, which writes into
  `<organism>/highSpeedSnpSearch` and so now disagrees with what the plugin reads. Flagged
  during Task 2. Left alone deliberately: it is a run-once snp-era prototype copier, not a
  deployment path — it hardcodes a `/eupath/data/htsSnpsPrototype/heterozygosityEnabled/`
  source, carries a hardcoded project→organism table the author named `$stupidHash`, and
  `die`s if the target already exists. It is not what produced the `dnaseq` directories and
  is not part of the variation pipeline. Recorded here so nobody later "fixes" it or mistakes
  it for how variation data reaches the webserver. **The open question it does raise —
  what populates production `<organism>/dnaseq` — is the data-deployment item above.**
- **The `webServiceMirror` override** needed to point the plugin at the test files (spec
  §5.1) — that belongs with the search that invokes the plugin, and the symlink bridging the
  missing `PlasmoDB/build-70` levels has already been created by hand.
