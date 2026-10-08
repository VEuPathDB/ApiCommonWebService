package ApiCommonWebService::HighSpeedSnpSearch::HsssScriptGenerator;

use strict;
use File::Basename;

sub new {
  my ($class) = @_;
  my $self = {};
  return bless($self, $class);
}

sub run {
  my ($self) = @_;
  $self->extractArgs();
  $self->writePerlWrapper();
  $self->makeStrainNamesToNumbersMap();
  $self->writeMainScript();
}

# extracts standard args, and returns arg list with extra args only
sub extractArgs {
  my ($self) = @_;
  $self->usage() unless scalar(@ARGV) >= 5;
  my @extraArgs;
  ($self->{strainFilesDir}, $self->{jobDir}, $self->{strainsAreNames}, $self->{outputScriptFile}, $self->{outputDataFile}, @extraArgs) = @ARGV;
  return @extraArgs;
}

sub getStandardArgsUsage {
  my ($self) = @_;
  return "strain_files_dir job_dir strains_are_names output_script_file output_data_file";
}

sub getStandardArgsHelp {
  my ($self) = @_;
  return "  - strains_file_dir:  the directory in which to find strain files.
  - job_dir:  a temp directory in which to create a set of unix fifos for this run.
  - strains_are_names: 0/1.  1=the strains in strains_list_file are strain names, not numbers as found in strains_file_dir.
  - output_script_file: the script
  - output_data_file: where to write the results";
}

sub writePerlWrapper {
  my ($self) = @_;
  writePerlWrapperFile($self->{outputScriptFile}, $self->{jobDir});
}

#
# write a perl wrapper because perl has the ninja power to change process group id.
# (plain function, also used by hsssGenerateMajorAllelesScript)
#
# the wrapper execs the bash script rather than running it as a child, so that signals sent
# to the wrapper's pid (eg, by java's Process.destroy() on timeout) land on the bash script,
# whose traps then kill the whole process group.  a perl parent would die alone, orphaning the tree.
#
sub writePerlWrapperFile {
  my ($outputScriptFile, $jobDir) = @_;

  open(O, ">$outputScriptFile") || die "Can't open output_file '$outputScriptFile' for writing\n";
  my $cmdString = $outputScriptFile =~ /^\//? "$outputScriptFile.bash" : "$jobDir/$outputScriptFile.bash";
  print O "#!/usr/bin/perl
# this wrapper sets the process group id so that all processes can be killed by traps, without killing parents such as tomcat
setpgrp(0,0);
\$cmd = \"$cmdString\";
exec('/bin/bash', \$cmd) or die \"perl could not run \$cmd \$!\";
";
  close(O);
  system("chmod +x $outputScriptFile");
}

#
# return bash code that makes the given fifos, and installs traps and a watchdog that
# kill every process in the job's process group (and remove the fifos) when the script
# exits for any reason: normal completion, error (set -e), a TERM/INT/HUP signal, or the
# death of its parent (eg, tomcat) or of the script itself (eg, kill -9).
#
# the process group is only killed if this script is its leader (ie, it was started via the
# perl wrapper's setpgrp), so that running the .bash file directly can't kill its caller.
# otherwise only the script's direct background jobs are killed.
#
# bash defers traps until the current foreground command finishes, so scripts using this
# should run long commands in the background and 'wait' for them.
#
sub getProcessCleanupBash {
  my (@fifos) = @_;

  return "mkfifo @fifos
hsssFifos=\"@fifos\"
hsssIsGroupLeader=0
if [ \"\$(ps -o pgid= -p \$\$ | tr -d ' ')\" = \"\$\$\" ]; then hsssIsGroupLeader=1; fi
hsssCleanup() {
  hsssExitCode=\$?
  trap '' TERM INT HUP
  if [ \$hsssIsGroupLeader = 1 ]; then
    kill -TERM -- -\$\$ 2>/dev/null || true
  else
    kill -TERM \$(jobs -p) 2>/dev/null || true
  fi
  rm -f \$hsssFifos
  exit \$hsssExitCode
}
trap hsssCleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
trap 'exit 129' HUP
# watchdog: kill the process group if this script dies or is orphaned by its parent
if [ \$hsssIsGroupLeader = 1 ]; then
  ( while [ \"\$(ps -o ppid= -p \$\$ | tr -d ' ')\" = \"\$PPID\" ]; do sleep 1; done; kill -TERM -- -\$\$ ) >/dev/null 2>&1 &
fi
";
}

sub makeStrainNamesToNumbersMap {
  my ($self) = @_;
  # if strains are names, make a mapping from strain names to strain numbers
  if ($self->{strainsAreNames}) {
    open(SN, "$self->{strainFilesDir}/strainIdToName.dat") || die "Can't open strain id mapping file '$self->{strainFilesDir}/strainIdToName.dat'\n";
    while(<SN>) {
      chomp;
      my ($num, $name) = split(/\t/);
      $self->{strainNameToNum}->{$name} = $num;
    }
    close(SN);
  }
}

sub writeMainScript {
  my ($self) = @_;

  my $outputScriptFile = $self->{outputScriptFile};

  # open target script file and write initial stuff to it
  open(my $o, ">$outputScriptFile.bash") || die "Can't open output_file '$outputScriptFile.bash' for writing\n";
  print $o "set -e\n";
  print $o "set -x\n";
  print $o "cd $self->{jobDir}\n";

  die "Error: '$self->{strainFilesDir}/referenceGenome.dat' does not exist or is empty " unless -s "$self->{strainFilesDir}/referenceGenome.dat";

  $self->writeMainScriptBody($o, $self->{outputDataFile});

  # print final stuff and clean up
  print $o "exit\n";
  close($o);
  system("chmod +x $outputScriptFile.bash");
}

# abstract method
sub writeMainScriptBody {
  my ($self, $fh, $outputDataFile) = @_;

  die "subclasses should override this";
}

# get strain num from strain name (unity mapping if there were no names on input)
sub getStrainNum {
  my ($self, $strain) = @_;

  my $strainNum = $strain;

  if ($self->{strainsAreNames}) {
    $strainNum = $self->{strainNameToNum}->{$strain};
    die "Can't find strain number for strain name '$strain'" unless $strainNum;
  }
  return $strainNum;
}

# abstract method
sub usage {
  my ($self) = @_;
  die "subclass must override this method";
}

1;
