package ApiCommonWebService::HighSpeedSnpSearch::HsssGenomicLocationFilterScriptGenerator;

use ApiCommonWebService::HighSpeedSnpSearch::HsssPolymorphismScriptGenerator;

@ISA = (ApiCommonWebService::HighSpeedSnpSearch::HsssPolymorphismScriptGenerator);
use strict;


sub getFinalCommandString {
  my ($self) = @_;

  my ($polymorphismThreshold, $unknownThreshold, $strainsListFile, $idPrefix, $idSuffix, $genomicLocationsFilterFile) = $self->extractArgs();

  return "hsssGenomicLocationsFilter $self->{strainFilesDir}/contigIdToSourceId.dat $genomicLocationsFilterFile";
}

# abstract method
sub usage {
  my ($self) = @_;

  my $standardArgsUsage = $self->getStandardArgsUsage();
  my $standardArgsHelp = $self->getStandardArgsHelp();

  my $polymorphismArgsUsage = $self->getPolymorphismArgsUsage();
  my $polymorphismArgsHelp = $self->getPolymorphismArgsHelp();

die "
Generate a bash script that will run a high-speed SNP search to find polymorphism among a set of input strain files,
and filter on the genomic locations provided.

usage: hsssGenerateGenomicLocationFilterScript $standardArgsUsage $polymorphismArgsUsage genomic_locations_filter_file

where:
$standardArgsHelp
$polymorphismArgsHelp
  - genomic_locations_filter_file:  tab delimited: seq_source_id, start, end. Must be sorted.

";
}

1;
