# High Speed SNP Search (HSSS) #

This documentation describes the High Speed SNP Search (HSSS) system.

Author's note:  when we get a chance, for clarity, we should rename these commands:
- hsssFindPolymorphic --> hsssFindSnpsInOneSetofStrains
- hsssFindMajorAlleles --> hsssFindSnpsInTwoSetsOfStrains

## The Data underlying the searches ##

The fundamental input to these searches is a set of resequenced genomes, here referered to as strains, that come from the same species.  They have been sequenced in short reads.  The reads are mapped to a reference genome, where they tile across it.  The height of the reads at a given location is called the "coverage."   If the coverage is less than five we ignore the reads at that location.  If there are more than five reads at a location, then we look at the largest number of those reads that agree with each other.  The percentage that these reads are of all the strain's reads at that location is called the "read frequency."  In the searches described below, the user will provide a threshold for this.  Locations with reads that do not have an allele at or above that threshold, or those that have no reads at all, are considered "unknown" and are ignored.

All this information for all the strains (at a given read frequency cutoff) is collated so that we have at each location of the reference genome, for every strain, one of the following:
  - a single allele that agrees with the reference genome
  - a single allele that differs from the reference genome
  - more than one allele, where one agrees with the reference genome
  - more than one allele, where all differ from the reference genome
  - unknown

Locations that have at least strain that has at least one allele that differs from the reference genome are considered a "global SNP location."  That is to say, this is a location where, when considering all available strains, there is a known SNP.  (If one were to consider only a subset of the strains, that location might not have a SNP.  That is what the searches do.)

Definition: A "global SNP" is a location on the genome in which, when considering all available strains, there is at least one strain with an allele that is different than the reference.

Definition: A "local SNP" is a location on the genome in which, when considering a subset of strains (those of interest), there is at least one strain with an allele that is different than the reference.  A local SNP must also be a global SNP.

## Search basics ##

The HSSS system supports a number of searches.  They divide into two types:  (1) given one group of strains (of a single species) and some parameters, find (local) SNPs among that group; (2) given two groups of strains (of a single species) find SNPs that differentiate the groups.  The first of those comes in different varieties based on some filters applied to resulting SNPS (genomic location or gene location).

The input to the searches are files that describe the alleles of each strain for each global SNP location.  For each strain there are a number of files, each at a predetermined read frequency cutoff (for example 20%, 40%, 60% or 80%). We use this quantization for performance reasons. The user chooses the input strains (either one set or two sets, depending on the search).  The C code processes the selected input files, merging pairs of files in parallel, and passing those to a next parallel tier of merging.  Because the files are sorted by SNP location, merging is efficient.  At the end of the merging the output has information for all involved strains in one stream, collated into SNP locations.  In the one group searches this stream is passed to filters to find SNPs that have the desired level of polymorphism, as well as other filters like gene locations.  In the two group search each group is reduced to one of those streams.  Each of those is passed to a program that collapses each to a consensus allele for each SNP.  Then those to consensi are compared to find SNPs between them.

All processing is acheived with unix streams.  The only disk i/o is reading the initial input files and writing the final result.  This is key to the speed of the system.  Most steps in the processing take in two streams and emit one.  Stdin does not work for that, as it is only a single stream.  Instead we use unix fifos.  These are simply named streams that appear to the consuming program like a file handle.

The structure of stream processing is a reducing tree of processes.  The size of the tree is determined by the number of strains the user chooses as input. The search system implements this tree by dynamically generating a shell script that is custom for a particular search.  The script passes arguments into the programs it calls that reflect the user's choice of search parameters.  For parallelism, the script puts all the processes in the background.  They are tied together by the FIFOs the consume and produce.

The programs that do the most intensive data processing, for example the merging of strain files, is coded in C for speed.  The final filtering is sometimes done in Perl.

The strain files are encoded in the most economical fashion, to keep the very large files as small as possible.  This reduces I/O which is the main expense in processing this kind of data.  The files are binary.  Strain names and sequence source_ids are encoded as simple integer IDs (a mapping of these IDs are stored in separate files).

The ultimate mapping of SNPs found by an HSSS search to SNP source_ids is based on the computability of SNP source_ids.  A SNP source_id is constructed as follows:  NGS. + sequence_source_id + location.


## Data files ##

The workflow creates a set of data files for use by HSSS.  They are stored in the webservices directory and are organized into a directory structure something like this:

```
PlasmoDB/
  Pfalciparum3D7/
    readFrequency20/
    readFrequency40/
    readFrequency60/
    readFrequency80/
  PvivaxBlah/
  ...
```

The readFrequency directories contain the data files.  The files are created with a particular read frequency cutoff as per the directory name.  The files in these directories are:

(1) strain files.  Each file represents the allele(s) of single strain at a particular read frequency threshold, at each location in the genome that has any variation at that location at that read frequency.  (Alleles that are not represented by that percentage of the reads at that location are excluded).   The file is named with the "HSSS strain ID", which an integer that has been mapped to the strain name.  The files describe the alleles of a given strain for each SNP location.  They are binary.  The rows are sorted by seq_id, location.  It is permitted for a SNP to have more than one row, which means that this strain had more than one allele at that location at this read frequency threshold.  This can only happen at read frequency thresholds <= 50%, and represents either heterozygosity (diploid organisms), unpure strains or sequencing errors.

The files are binary with 4 columns:
   - seq_id. 2 bytes. the HSSS seq ide, an integer mapped to the sequence source id.  (They start at 1.)
   - location.  4 bytes.  the location on the sequence of SNP
   - allele.  1 byte.  1=A, 2=C, 3=G, 4=T, 0=unknown (alleles with other IUPAC codes, eg, Y, are set to 0, ie, ignored)
   - product. 1 byte. the ascii encoding of the upper case product, eg, A=65. \*=42 (stop codon) 0=non-coding.

If an allele is "like reference," ie, the same as the reference genome (that genome to which all the strain sequences were mapped), then that allele is not included in the strain file.  This is a form of compression.  (The gamble is that there are fewer unknown locations than like-reference, and that is increasingly true as there are more strains causing more SNP locations, given that coverage is usually pretty good in NGS resequencing).  If a strain has more than one allele at a location then both are included in the file even if one is like-reference. If the reference has an IUPAC code for an ambiguous base and the strain has that same code, then the strain is considered like-reference, and no row is included in the file.

(2) reference genome file. Same columns as strain files.  This file describes the alleles in the reference genome.  (The genome to which all the strains were aligned.)  Every known SNP for the organism has a row in this file.  The only alleles with value 0, ie, unknown, are those where the allele in the genome is an IUPAC ambiguous base pair code.

(3) strain IDs file.  Maps integer strain IDs to strain names. The IDs start with 1.

(4) sequence IDs file.  Maps integer sequence IDs to sequence source_ids.  The IDs start with 1.


## Runtime streams ##

These streams are produced as an HSSS search executes.

(1) merged strain streams.  Produced by hsssMergeStrains.  These files represent the merging of 0, 1 or more strain files.  Like strain files they are binary and sorted by seq_id and location.  For a particular SNP, all alleles from a given strain are in contiguous rows but otherwise the strains are not ordered.  The files have five columns.  The first four are the same as in a strain file; the fifth is the strain_id (2 bytes).  These files introduce a form of compression not used in the strain files.  If a given SNP has more than one unknown, the multiple unknowns may be compressed into a single row.  That row will have a -1 in the product column and a count of unknowns in the strain_id column.  A SNP may have 0, 1 or more than 1 such compressed rows.

(2) consenensus streams.  Produced by hsssMergedToConsensus. These files represent the consensus of a set of strains.  For each SNP, the set of alleles among all the strains at that SNP is reduced to a major and minor allele, where the former is the most popular and the latter the second most popular.  If there are ties the choices are arbitrary.  A SNP is not reported in this file if not enough strains have reads at that location (as specified by the unknowns threshold parameter to hsssMergedToConsensus). The columns are:
   - seq_id. same as in strain files
   - location.  same as in strain files
   - major_allele. 1 byte.  same encoding as strains file (except never 0).
   - major_allele_product.  1 byte.  same encoding as strains file.
   - major_allele_product_is_variable. 1 byte.  0=all major alleles have the same product.  1=they do not.
   - minor_allele.
   - minor_allele_product.
   - minor_allele_product_is_variable.
   - major_allele_per_ten_thousand. 2 bytes.  percent of alleles that are the major allele, times 100 for precision
   - minor_allele_per_ten_thousand.
   - is_triallelic. 1 byte.  1=is triallelic, ie, has more than two alleles.

(3) SNPs streams (one group searches).  Produced by hsssFindPolymorphic.  Represents the SNPs found in a single group search.  It is tab delimited text, with these columns:
   - seq_id. same as in strain files
   - location.  same as in strain files
   - knownPercent.  a float, to one decimal point. The % of strains having reads at this location.
   - polymorphismsPercent. a float, to one decimal point.  The % of known alleles that are not the major allele.
   - productClass.
       - 0=noncoding (no allele has a product or stop codon)
       - 1=synonymous (all products agree)
       - 2=nonsynonymous (not all products agree)
       - negative=nonsense (at least one allele has a nonsense product)

(4) SNPs streams (two groups search).  Produced by hsssFindMajorAlleles. Represents the SNPs found across two groups of sequences.  Each group (A and B) has been reduced to a consensus. This output shows details of each of those consensuses at SNP locations where they differ from each other.  Tab delimited text, with these columns:
   - group A seq id.  HSSS seq ID.
   - group A location.
   - group A major allele.
   - group A major allele percent.  The percent of alleles in the consensus that are the major allele.
   - group A is triallelic.
   - group A major product.
   - group A major product is variable.
   - same colums for group B.

## Final output files ##

The final output of a search is written to a directory accessed by the WSF plugin.  The plugin passes the rows to the WSF framework, unmodified.  The final output files contain the values the user sees.  A given search has one of these three types of output file:

(1) SNPs found among a single set of strains.  This is output by searches that take as input a single set of strain files.  Some of these searches have filters that remove SNPs from the stream (eg, by genomic location), and this output is after those filters.  This file has the same information as the SNPs stream (one group searches), but in a user-readable format.  The SNP source ID is formed from the raw seq_id and location columns, and the productClass is converted from a number code to words.

(2) SNPs found among two sets of strains. This is output by the search that takes as input two sets of strain files.  This file has the same information as the SNPs stream (two group searches), but in a user-readable format.  The SNP source ID is formed from the raw seq_id and location columns, and the productClass is converted from a number code to words.

(3) Genes that have SNPs found among a single set of strains.  This is produced by analyzing a stream of SNPs (single set of strains), looking at the SNPs within gene boundaries, and outputting the genes whose SNPs meet the specified criteria, as well as summary statistics a bout those genes' SNPs.  Tab delimited text:
   - gene id
   - SNP density
   - ratio non-syn/syn
   - syn count
   - non-syn count
   - non coding count
   - nonsense count
   - SNPs count


## Types of searches ##

There are two types of searches: those that process a single set of strains and those that process two sets of strains.

In the first case the user selects a set of strains and a read frequency threshold.   That determines a set of input files (one per strain at that read frequency).  These files are merged (in parallel) to form a single stream containing the collated information for each global SNP location.  That stream is processed to identify either SNPs or genes.

Here is an illustration, where the user has selected four strains.  (The tree would expand to the left if they had selected more):

```
strain 1  \
           ==merge==>  1,2 merged  \                                                         / ==optional filters==> SNPs
strain 2  /                         \                                                       /
                                     ==merge==>  1,2,3,4 merged  ==find SNPs==>  SNPs (text)
strain 3  \                         /                                                       \
           ==merge==>  3,4 merged  /                                                         \ ==optional filters==> SNPs ==find genes==> Genes
strain 4  /
```


In the second case the user selects two sets of strains, and a read frequency threshold for each.  Each set is merged into a consensus.  These are compared to find SNPs between them, as well as the major allele, major product, minor allele, minor product and other statistics.

```
set 1: strain 1  \
                  ==merge==>  1,2 merged ==find consensus==> consensus \
set 1: strain 2  /                                                      \
                                                                         ==find SNPs and major alleles==> SNPs
set 2: strain 3  \                                                      /
                  ==merge==>  3,4 merged ==find consensus==> consensus /
set 2: strain 4  /
```


## Sample search runner script ##

The following is a sample script generated to run a specific search, with specific parameter values provided by the user. It searches for Genes that contain SNPs found in a specified group of strains.   The user has selected:
  - 9 strains (with internal IDs 13,21,16,5,29,3,6,11 and 8)
  - a read frequency threshold of 20 and some other parameters.
  - a 0% minimum polymorphism percent (ie, if there is any polymorphism at a location, the SNP is kept)
  - an allowance of up to 8 unknowns for a SNP to be kept.
  - additional parameters to control the filtering of Genes based on a Gene's SNP characteristics (eg, SNP density)

The process cleanup preamble (from `mkfifo` through the watchdog's closing `fi`) is shortened here.  See [Process cleanup](#process-cleanup) for the full text.

```bash
set -e
set -x
cd $HSSS_WORKING_DIRS/hsssFindPolymorphisms.1408730457121
mkfifo fifo1 fifo2 fifo3 fifo4 fifo5 fifo6 fifo7 fifo8 fifo9
hsssFifos="fifo1 fifo2 fifo3 fifo4 fifo5 fifo6 fifo7 fifo8 fifo9"
# ... rest of the process cleanup preamble ...
hsssMergeStrainsWrapper fifo1 $HSSS_FILES/EhistolyticaHM1IMSS/readFreq20/13 13 $HSSS_FILES/EhistolyticaHM1IMSS/readFreq20/21 21  &
hsssMergeStrainsWrapper fifo2 $HSSS_FILES/EhistolyticaHM1IMSS/readFreq20/16 16 $HSSS_FILES/EhistolyticaHM1IMSS/readFreq20/5 5  &
hsssMergeStrainsWrapper fifo3 $HSSS_FILES/EhistolyticaHM1IMSS/readFreq20/29 29 $HSSS_FILES/EhistolyticaHM1IMSS/readFreq20/3 3  &
hsssMergeStrainsWrapper fifo4 $HSSS_FILES/EhistolyticaHM1IMSS/readFreq20/6 6 $HSSS_FILES/EhistolyticaHM1IMSS/readFreq20/11 11  &
hsssMergeStrainsWrapper fifo5 $HSSS_FILES/EhistolyticaHM1IMSS/readFreq20/8 8 fifo1 0  &
hsssMergeStrainsWrapper fifo6 fifo2 0 fifo3 0  &
hsssMergeStrainsWrapper fifo7 fifo4 0 fifo5 0  &
hsssMergeStrainsWrapper fifo8 fifo6 0 fifo7 0  &
hsssFindPolymorphic fifo8 $HSSS_FILES/EhistolyticaHM1IMSS/readFreq20/referenceGenome.dat 9 0 8 | hsssGeneCharacteristicsFilter $HSSS_FILES/EhistolyticaHM1IMSS/readFreq20/contigIdToSourceId.dat geneLocations.txt all 8 -1 0 -1 0 100011 > $HSSS_WORKING_DIRS/hsssFindPolymorphisms.1408730457121/results &
wait $!
exit
```

## Process cleanup ##

A search runs many background processes joined by FIFOs.  If the script stops early (an error, the Java timeout, Tomcat shutting down) and those processes are not killed, they are left behind.  A merge process blocked opening a FIFO whose reader is gone waits forever.  The cleanup code makes sure that every process in a search's tree is killed, and its FIFOs removed, however the search ends.

The code is shared by all the script generators and lives in `lib/perl/HsssScriptGenerator.pm`:
  - `writePerlWrapperFile()` writes the Perl wrapper that Java runs.
  - `getProcessCleanupBash(@fifos)` returns the bash preamble that makes the FIFOs and installs the traps and the watchdog.

`HsssPolymorphismScriptGenerator.pm` (and the gene and genomic location generators that subclass it) and `bin/hsssGenerateMajorAllelesScript` all use them.

### The process group ###

The WSF plugin (`HighSpeedSnpSearchAbstractPlugin`) runs the wrapper with a two minute timeout.  On timeout Java calls `Process.destroy()`, which sends SIGTERM to the one pid it started.  The wrapper is:

```perl
#!/usr/bin/perl
# this wrapper sets the process group id so that all processes can be killed by traps, without killing parents such as tomcat
setpgrp(0,0);
$cmd = "/path/to/jobDir/findPolymorphisms.bash";
exec('/bin/bash', $cmd) or die "perl could not run $cmd $!";
```

  - `setpgrp(0,0)` makes the wrapper the leader of a new process group, whose ID is the wrapper's pid.  Tomcat is not in it, so killing the group can't touch Tomcat.  Every process the script starts joins this group.
  - `exec` replaces Perl with bash without changing the pid.  So the bash script is the process Java knows about, and Java's SIGTERM goes straight to it.

(An earlier version used `system()`, which ran bash as a child of Perl.  `destroy()` then killed only Perl, and the bash tree kept running.)

### The generated cleanup code ###

This is the output of `getProcessCleanupBash("fifo1", "fifo2", "fifo3")`.  It goes right after the `cd` into the job directory, before any process is started:

```bash
mkfifo fifo1 fifo2 fifo3
hsssFifos="fifo1 fifo2 fifo3"
hsssIsGroupLeader=0
if [ "$(ps -o pgid= -p $$ | tr -d ' ')" = "$$" ]; then hsssIsGroupLeader=1; fi
hsssCleanup() {
  hsssExitCode=$?
  trap '' TERM INT HUP
  if [ $hsssIsGroupLeader = 1 ]; then
    kill -TERM -- -$$ 2>/dev/null || true
  else
    kill -TERM $(jobs -p) 2>/dev/null || true
  fi
  rm -f $hsssFifos
  exit $hsssExitCode
}
trap hsssCleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
trap 'exit 129' HUP
# watchdog: kill the process group if this script dies or is orphaned by its parent
if [ $hsssIsGroupLeader = 1 ]; then
  ( while [ "$(ps -o ppid= -p $$ | tr -d ' ')" = "$PPID" ]; do sleep 1; done; kill -TERM -- -$$ ) >/dev/null 2>&1 &
fi
```

**Group leader check.**  `hsssIsGroupLeader` is 1 only when the script's process group ID equals its own pid, which is true when it was started through the wrapper.  If someone runs the `.bash` file directly from a shell, the script is in that shell's group, and a group kill would kill the shell too.  In that case the script kills only its own background jobs (`jobs -p`) and doesn't start the watchdog.

**`hsssCleanup`, the EXIT trap.**  Bash runs the EXIT trap whenever the shell exits: at the final `exit`, when `set -e` stops the script after a failed command, or from `exit` in one of the signal traps below.  The function:
  1. Saves `$?`, so the script still exits with its real status.  Java uses that status to decide whether the search failed.
  2. Ignores TERM, INT and HUP.  The next step sends SIGTERM to the whole group, and the script is in the group.  Without this it would kill itself before removing the FIFOs.
  3. Sends SIGTERM to the process group (`kill -TERM -- -$$`; a negative pid means the group with that ID).  Every merge, consensus, filter and watchdog process gets it, including ones blocked opening a FIFO.  SIGTERM is used rather than SIGINT because a non-interactive bash starts its background jobs with SIGINT ignored.
  4. Removes the FIFOs.
  5. Exits with the saved status.

**Signal traps.**  TERM, INT and HUP each run `exit` with the usual 128 + signal number (143, 130, 129).  The `exit` runs the EXIT trap, so a signal gets the same cleanup as a normal exit.  Without these traps bash would be killed by the signal's default action and the EXIT trap would never run.

**Watchdog.**  Traps can't help if the script gets SIGKILL, and nothing signals the script if the JVM dies.  The watchdog handles both.  It is a background subshell, in the same process group, that checks once a second whether the script's parent has changed:
  - In a subshell, `$$` and `$PPID` still mean the main script's pid and the script's original parent (the JVM, because of the `exec`).
  - `ps -o ppid= -p $$` prints the script's current parent.  The loop keeps going while that is still `$PPID`.
  - If the JVM dies, the script is re-parented (to init or a subreaper), so its parent pid changes.
  - If the script itself dies (eg, `kill -9`), `ps` prints nothing, which doesn't match either.
  - Either way the loop ends and the watchdog sends SIGTERM to the whole group, itself included.

In a normal run the watchdog is killed by `hsssCleanup` along with everything else.

**Waiting on the final pipeline.**  The generators start the last pipeline in the background and then `wait` for it:

```bash
hsssFindPolymorphic fifo8 ... | hsssGeneCharacteristicsFilter ... > results &
wait $!
```

Bash doesn't run a trap while a foreground command is running; it waits for the command to finish first.  If the final pipeline ran in the foreground, Java's SIGTERM would do nothing until the pipeline ended, which with a stuck FIFO might be never.  The `wait` builtin returns as soon as a trapped signal arrives, so the trap runs right away.  `wait $!` also returns the pipeline's exit status, so a failed pipeline still stops the script through `set -e`.

**Errors in a merge step.**  `hsssMergeStrainsWrapper` has its own trap:

```bash
trap "kill -TERM 0" ERR # kill process group on error
```

`kill 0` signals the caller's own process group, which is the search's group.  If `hsssMergeStrains` fails, the main script gets SIGTERM along with everything else, its TERM trap runs `exit 143`, and `hsssCleanup` does the rest.

### What happens in each case ###

| How the search ends | What does the cleanup |
|---|---|
| Normal completion | final `exit` → EXIT trap |
| A command in the script fails (`set -e`) | shell exits → EXIT trap |
| `hsssMergeStrains` fails | wrapper's ERR trap sends SIGTERM to the group → TERM trap → EXIT trap |
| Java timeout (`Process.destroy()`) | SIGTERM to the script → TERM trap → EXIT trap |
| `kill`, Ctrl-C or hangup sent to the script | TERM / INT / HUP trap → EXIT trap |
| Tomcat/JVM dies or is killed | script is re-parented → watchdog kills the group |
| Script gets `kill -9` | script is gone → watchdog kills the group |
| `kill -9` sent to the whole group | nothing runs.  Every process is already dead so nothing is orphaned, but the FIFOs are left in the job directory. |

To check for leftovers, start a search with a large strain list and `kill` the pid Java started (after the `exec` it shows as `/bin/bash .../<command>.bash`).  Within about a second `pgrep -a hsss` should print nothing.

## How a search works ##

## Test suite ##

## Debugging ##

In wdk.log4j, find a line like this for the search you just ran:

```
173432257 [INFO ] [12/Feb/2015:14:25:52] 128.91.68.248 - org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch.HighSpeedSnpSearchAbstractPlugin:117 - Invoking findPolymorphisms plugin execute() for job /var/www/Common/tmp/highSpeedSnpSearch/hsssFindPolymorphisms.1423769152120$
```

It shows you the job directory.  Go to that directory.  There you will find something like this:

```
$ ls |m
findPolymorphisms*
findPolymorphisms.bash*
results
strains
```

Where:
  - findPolymorphisms:       a small wrapper that does some tricks to fool unix into proper killing of jobs (see [Process cleanup](#process-cleanup))
  - findPolymorphisms.bash:  the generated script that runs the job.
  - results:                 the results
  - strains:                 the input strains

Typically, to debug, you need to examine all the intermediate data.  During the run, these were all transient streams that feed into the final step.  You need to make each one be a saved file.  To do so:

1) copy the job dir to a place in your home, or any place you easily have permission to write files to:
   ```
   cp -r /var/www/Common/tmp/highSpeedSnpSearch/hsssFindPolymorphisms.1423769152120 ~/junk
   ```

2) cd to that dir.

3) edit findPolymorphisms.bash to make it write the streams to files:

   - a) remove the `cd` line, and the process cleanup preamble: everything from the `mkfifo` line through the `fi` that closes the watchdog block.  It starts like this:
     ```
     cd /var/www/Common/tmp/highSpeedSnpSearch/hsssFindPolymorphisms.1423769152120
     mkfifo fifo1 fifo2 fifo3 fifo4 fifo5
     hsssFifos="fifo1 fifo2 fifo3 fifo4 fifo5"
     ...
     ```

   - b) replace fifo with tmp.  now all the stuff that was streamed to/from fifos will be written to and read from files

   - c) remove the &'s so that nothing runs in bgd, and remove the `wait $!` line

   - d) the last command of the script might include some pipes.  change it so that each piece writes to a file, and the next piece reads from that file.

   - e) the final step writes to a results file.  remove the path, so that results is local to your current dir

4) now you can run findPolymorphisms.bash, and it will put all its files into your local dir.

5) some of the files are binary.  For each type of binary file, there is a command that dumps that file.  To find the dumpers, use UNIX auto complete:
   ```
   $hsssDump
   hsssDumpConsensusFile  hsssDumpRefStrain      hsssDumpStrains
   ```


---

```bash
set -e
set -x
cd /var/www/Common/tmp/highSpeedSnpSearch/hsssFindPolymorphisms.1406829738062
mkfifo fifo1
hsssFifos="fifo1"
# ... rest of the process cleanup preamble ...
hsssAddStrainId 1 < /var/www/Common/apiSiteFilesMirror/webServices/TriTrypDB/build-22/TbruceiTREU927/highSpeedSnpSearch/readFreq40/1 > fifo1 &
hsssFindPolymorphic fifo1 /var/www/Common/apiSiteFilesMirror/webServices/TriTrypDB/build-22/TbruceiTREU927/highSpeedSnpSearch/readFreq40/referenceGenome.dat 1 0 0 | hsssReconstructSnpId /var/www/Common/apiSiteFilesMirror/webServices/TriTrypDB/build-22/TbruceiTREU927/highSpeedSnpSearch/readFreq40/contigIdToSourceId.dat 1 NULL NULL >/var/www/Common/tmp/highSpeedSnpSearch/hsssFindPolymorphisms.1406829738062/results &
wait $!
exit
```
