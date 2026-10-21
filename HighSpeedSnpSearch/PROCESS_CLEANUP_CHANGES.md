# HighSpeedSnpSearch: subprocess cleanup changes

The whole process tree is now killed when a search ends, fails, gets TERM/INT/HUP, or loses its parent.

**Status:** not run yet. The only checks so far are `perl -c` on the changed Perl files and `bash -n` on the generated bash; both pass. Nothing is committed.

## Why processes were left behind

Java runs a Perl wrapper, which calls `setpgrp` and then `system()`s the bash script. On timeout, Java's `process.destroy()` sends TERM to the Perl wrapper only. Perl dies and the bash tree is left running. The old bash trap only removed the FIFOs, so merge writers blocked opening a deleted FIFO hung forever.

## What changed

### `lib/perl/HsssScriptGenerator.pm`

- **New `writePerlWrapperFile()`:** the generated wrapper still calls `setpgrp(0,0)`, then `exec`s `/bin/bash` instead of using `system()`. The bash script now has the pid Java knows about, so Java's TERM reaches it directly.
- **New `getProcessCleanupBash()`:** emits the shell code that runs at the top of each generated script:
  - **EXIT trap:** kills the job's whole process group, then removes the FIFOs. It runs on normal exit, on a `set -e` failure, and on any signal that is caught. While cleaning up, the script ignores TERM so it doesn't kill itself partway through.
  - **TERM/INT/HUP traps:** they `exit`, which runs the EXIT trap.
  - **Watchdog:** a background loop that checks once a second whether the script's parent has changed or the script has disappeared, and if so kills the group. This covers what traps can't: Tomcat/JVM dying, or the script being sent `kill -9`.
  - **Safety guard:** the group is only killed if the script leads its own group, which is true when started through the wrapper. If someone runs the `.bash` file directly, only its own background jobs are killed, never the caller's group.

### `lib/perl/HsssPolymorphismScriptGenerator.pm` and `bin/hsssGenerateMajorAllelesScript`

- Both use the shared helpers. The major-alleles script had its own copy of the wrapper and FIFO code.
- The final pipeline now runs as `... &` followed by `wait $!`. Bash doesn't run traps while a foreground command is running, so without this a signal would be ignored until the pipeline finished, which might be never. `wait $!` returns the pipeline's exit status, so `set -e` behaves as before.

### Unchanged

`hsssMergeStrainsWrapper` keeps its `trap "kill -TERM 0" ERR`. With the new group handling, an error there kills the whole job cleanly.

## Things to know

- **Can't handle:** a `kill -9` sent to the entire process group at once. Nothing survives to clean up, but nothing is orphaned either.
- **The test suite won't pass yet, for an unrelated reason:** in the baseline run before this change, the installed `$GUS_HOME/bin/hsssReconstructSnpId` is out of date. It writes `contig.loc` IDs where the source and expected output use `contig_loc`, so the suite fails at its second comparison. A `bld ApiCommonWebService` should fix that and install these changes.
- **To confirm the fix:** after installing, run a search with a large strain list and `kill` the wrapper's pid. Then `pgrep -a hsss` should come back empty within about a second.
- **Optional extra layer in WSF:** `AbstractPlugin.invokeCommand` (in the WSF repo) could also call `process.descendants().forEach(ProcessHandle::destroy)` on timeout. With these changes it isn't required.
