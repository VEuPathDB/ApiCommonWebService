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
  my ($self, $argsRef) = @_;
  $self->usage() unless scalar(@$argsRef) >= 5);
  my @extraArgs;
  ($self->{strainFilesDir}, $self->{jobDir}, $self->{strainsAreNames}, $self->{outputScriptFile}, $self->{outputDataFile}, @extraArgs) = @ARGV;
  $self->extraArgs = \@extraArgs;
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

#
# write a perl wrapper because perl has the ninja power to change process group id
#
sub writePerlWrapper {
  my ($self) = @_;

  my $outputScriptFile = $self->{outputScriptFile};
  open(O, ">$outputScriptFile") || die "Can't open output_file '$outputScriptFile' for writing\n";
  my $cmdString = $outputScriptFile =~ /^\//? "$outputScriptFile.bash" : "$self->{jobDir}/$outputScriptFile.bash";
  print O "#!/usr/bin/perl
# this wrapper sets the process group id so that all processes can be killed by traps, without killing parents such as tomcat
setpgrp(0,0);
\$cmd = \"$cmdString\";
system(\$cmd) && die \"perl could not run \$cmd \$?\";
";
  close(O);
  system("chmod +x $outputScriptFile");
}

sub makeStrainNamesToNumbersMap {
  my ($self) = @_;
  # if strains are names, make a mapping from strain names to strain numbers
  my %strainNameToNum;
  if ($self->{strainsAreNames}) {
    open(SN, "$self->{strainFilesDir}/strainIdToName.dat") || die "Can't open strain id mapping file '$self->{strainFilesDir}/strainIdToName.dat'\n";
    while(<SN>) {
      chomp;
      my ($num, $name) = split(/\t/);
      $strainNameToNum{$name} = $num;
    }
    close(SN);
  }
  return %strainNameToNum;
}

sub writeMainScript {
  my ($self) = @_;

  my $outputScriptFile = $self->{outputScriptFile};

  # open target script file and write initial stuff to it
  open(my $o, ">$outputScriptFile.bash") || die "Can't open output_file '$outputScriptFile.bash' for writing\n";
  print $o "set -e\n";
  print $o "set -x\n";
  print $o "cd $jobDir\n";

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

