package ApiCommonWebService::HighSpeedSnpSearch::HsssPolymorphismScriptGenerator;

use ApiCommonWebService::HighSpeedSnpSearch::HsssScriptGenerator;

@ISA = (ApiCommonWebService::HighSpeedSnpSearch::HsssScriptGenerator);
use strict;
use File::Basename;

sub writeMainScriptBody {
  my ($self, $fh, $outputDataFile) = @_;

  my ($polymorphismThreshold, $unknownThreshold, $strainsListFile) = $self->extractArgs();

  # read strains list file and make a queue of strains to process from it
  open(S, $strainsListFile) || die "Can't open strains_list_file '$strainsListFile'\n";
  my @mergeQueueOriginal = <S>;
  close(S);
  my @mergeQueue = map { chomp; "$self->{strainFilesDir}/" . $self->getStrainNum($_)} @mergeQueueOriginal;
  my $strainsCount = scalar(@mergeQueue);

  # write making of fifos and a trap to remove them
  my $fifoCount = $strainsCount;
  my $fifoCursor = 0;
  my $fifoPrefix = "fifo";
  print $fh "mkfifo ";
  for (my $i = 1; $i <= scalar(@mergeQueue); $i++) {
    print $fh "$fifoPrefix$i ";
  }
  print $fh "\n";
  print $fh "trap \"rm ";
  for (my $i = 1; $i <= $fifoCount; $i++) {
    print $fh "$fifoPrefix$i ";
  }
  print $fh "\" EXIT TERM\n";

  # print out merge commands and then the find polymorphic command
 my $output = $outputDataFile? ">$outputDataFile" : "";
  my $finalCommand = $self->getFinalCommandString();

  while (1) {
    # if the merge queue has more than one stream in it, merge two at a time
    if (scalar(@mergeQueue) > 1) {
      my $input1 = shift(@mergeQueue);
      my $input2 = shift(@mergeQueue);
      $fifoCursor++;
      push(@mergeQueue, "$fifoPrefix$fifoCursor");
      my $strain1 = basename($input1);
      my $strain2 = basename($input2);
      $strain1 = 0 if $strain1 =~ /$fifoPrefix/;
      $strain2 = 0 if $strain2 =~ /$fifoPrefix/;
      print $fh "hsssMergeStrainsWrapper $fifoPrefix$fifoCursor $input1 $strain1 $input2 $strain2  &\n";
    }
    # if only one stream in the queue, it is the result of all the merging.  print find polymorphism command
    else {
      my $allMerged = shift(@mergeQueue);

      # if there was only one file input, then the merged stream needs a strain id column
      if (scalar(@mergeQueueOriginal) == 1) {
	my $singleInputFile = $allMerged;
	my $strainId = basename($singleInputFile);
	$fifoCursor++;
	print $fh "hsssAddStrainId $strainId < $singleInputFile > $fifoPrefix$fifoCursor &\n";
	$allMerged = "$fifoPrefix$fifoCursor";
      }

      print $fh "hsssFindPolymorphic $allMerged $self->{strainFilesDir}/referenceGenome.dat $strainsCount $polymorphismThreshold $unknownThreshold | $finalCommand $output\n";
      last;
    }
  }
}

sub getFinalCommandString {
  my ($self) = @_;

  my ($polymorphismThreshold, $unknownThreshold, $strainsListFile, $seqFilter, $minLoc, $maxLoc) = $self->extractArgs();

  return "hsssReconstructSnpId $self->{strainFilesDir}/contigIdToSourceId.dat 1 $seqFilter $minLoc $maxLoc";
}

sub getPolymorphismArgsUsage{
  return "polymorphism_threshold unknown_threshold strains_list_file";
}

sub getPolymorphismArgsHelp {
  return "  - polymorphism_threshold: see the hsssFindPolymorphism program for documentation on this argument
  - unknown_threshold: see the hsssFindPolymorphism program for documentation on this argument
  - strains_list_file: a list of strain files.  Each must have an integer as a name (an ID for that strain).
"
}

# abstract method
sub usage {
  my ($self) = @_;

  my $standardArgsUsage = $self->getStandardArgsUsage();
  my $standardArgsHelp = $self->getStandardArgsHelp();

  my $polymorphismArgsUsage = $self->getPolymorphismArgsUsage();
  my $polymorphismArgsHelp = $self->getPolymorphismArgsHelp();

die "
Generate a bash script that will run a high-speed SNP search to find polymorphism among a set of input strain files.

usage: hsssGeneratePolymorphismScript $standardArgsUsage $polymorphismArgsUsage [seq_id min_loc max_loc]

where:
$standardArgsHelp
$polymorphismArgsHelp
  - seq_id:  optional encoded sequence id.  if present, return only SNPs on this sequence.
  - min_loc: required if seq_id provided.  return only SNPs at this location or larger
  - max_loc: required if seq_id provided.  return only SNPs at this location or smaller

";
}

1;
