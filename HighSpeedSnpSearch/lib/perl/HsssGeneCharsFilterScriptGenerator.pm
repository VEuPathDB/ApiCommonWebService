package ApiCommonWebService::HighSpeedSnpSearch::HsssGeneCharsFilterScriptGenerator;

use ApiCommonWebService::HighSpeedSnpSearch::HsssPolymorphismScriptGenerator;

@ISA = (ApiCommonWebService::HighSpeedSnpSearch::HsssPolymorphismScriptGenerator);
use strict;


sub getFinalCommandString {
  my ($self) = @_;

  my ($polymorphismThreshold, $unknownThreshold, $strainsListFile, $reconstructCmdName, $idPrefix,$idSuffix, $geneLocationsFile, $snpClass, $snpsMin, $snpsMax, $dndsMin, $dndsMax, $densityMin, $densityMax) = $self->extractArgs();

  return "hsssGeneCharacteristicsFilter $self->{strainFilesDir}/contigIdToSourceId.dat $geneLocationsFile $snpClass $snpsMin $snpsMax $dndsMin $dndsMax $densityMin $densityMax";
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

usage: hsssGenerateGeneCharsFilterScript $standardArgsUsage $polymorphismArgsUsage gene_locations_filter_file snp_class snps_min snps_max dnds_min dnds_max density_min density_max

where:
$standardArgsHelp
$polymorphismArgsHelp
  - gene_locations_filter_file: tab delimited:  contig_source_id, start, end, gene_source_id.   Must be sorted by location
  - snp_class:  coding, noncoding, synonymous, nonsynonymous, nonsense
  - snps_min: min percent of SNPs in the gene that belong to the specified class
  - snps_min: max percent of SNPs in the gene that belong to the specified class
  - dnds_min: min dn/ds ratio
  - dnds_min: max dn/ds ratio
  - density_min: min SNPs density
  - density_max: max SNPs density


";
}

1;
