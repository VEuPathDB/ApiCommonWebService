#!/usr/bin/perl
use strict;

use lib "$ENV{GUS_HOME}/lib/perl";

use DBI;
use DBD::Oracle;
use File::Temp qw/ tempfile /;
use Data::Dumper;

# ----------------------------------------------------------------------
# Input
# ----------------------------------------------------------------------
# ARGV[0] - id sql
# ARGV[1] - use orthology
# ARGV[2] - FDR 
# ARGV[3] - db_connection
# ARGV[4] - db_login
# ARGV[5] - db_password
#print STDERR join("\n", @_) . "\n";

my $idSql = $ARGV[0];


my $FDR = $ARGV[2];
my $useOrthology = $ARGV[1];

# read the db connection information
my $dbConnection = $ARGV[3];
my $dbLogin = $ARGV[4];
my $dbPassword = $ARGV[5];

#print STDERR "db connection $dbConnection db login $dbLogin db Password $dbPassword\n";

# setup DBI connections
my $dbh = DBI->connect($dbConnection, $dbLogin, $dbPassword) or die "Cannot connect to database";

$dbh->{RaiseError} = 1;
$dbh->{LongTruncOk} = 0;
$dbh->{LongReadLen} = 10000000;

my @userGeneList = &getValidGeneList($dbh, $idSql);
#print STDERR "gene is is now coming in as @userGeneList";

#my @userGeneList = @{$userGeneList};
if (@userGeneList <20) {
    die "This analysis tool requires a minimum of 20 genes from the same organ#ism to proceed, only found @userGeneList";
}

#determine organism of list - how to do this
#my $list = join(',' , @userGeneList); 
my $userOrganism;
my $sql = <<EOSQL;
select distinct dgl.organism 
    from ApidbTuning.DatasetGeneList dgl,
    ($idSql) idsql
    where idsql.gene_source_id = dgl.source_id
EOSQL
    
    my $sth = $dbh->prepare($sql);

$sth->execute();
my $line;
$sth->bind_columns(undef, \$line);

my @orgs; 
while( $sth->fetch() ) {
    my $org = $line;
    push @orgs, $org; 
}
$sth->finish();

if (@orgs >=2) {
    
    die "The gene list given contains gene ids from multiple organisms, only one organism is permitted"; 
}
else {
    $userOrganism = $orgs[0];
#    print STDERR "User organism is $userOrganism\n";
}

# get hash of off the RNASeq experiments and the corresponding organism 
my %RNASeqHash;
my %organismHash;
my $sql = <<EOSQL;
select distinct organism || '\t' || dataset_name 
    from ApidbTuning.DatasetGeneList 
EOSQL
    
    my $sth = $dbh->prepare($sql);

$sth->execute();
my $line;
$sth->bind_columns(undef, \$line);


while( $sth->fetch() ) {
    my @temps = split "\t", $line;
    my $org = $temps[0];
    my $dataset = $temps[1];
    $RNASeqHash{$dataset} = $org;
    $organismHash{$org}=1;
}
$sth->finish();

# set of 2  sub routines for each dataset that a) creates the ranked list b) runs GSEA 

#I think I need to actually do the transform just once 

# so check user organism and wthin same here first to lower memory usage 

# 
my %transformedListHash;

foreach my $element(keys %organismHash) {
 #   print STDERR "organisms of all are $element\n";
    if ($element eq $userOrganism) {
	$transformedListHash{$element} = \@userGeneList;
    }
    else {
#	my $OrgToTransTo = $RNASeqHash{$test};
	my $convertedList = &transformByOrtholog($dbh, $element, $idSql);
	$transformedListHash{$element} = $convertedList;
#	print STDERR  "Getting to here $convertedList\n";
    }
}

# here if the user is only interested in same organism then I can reduce this down a lot. 

## try setting home
system("export HOME=/var/www/Common/tmp/wdkStepAnalysisJobs/apicommdevn");

my %matchedDatasets; 
my $count = 0;
foreach my $test (keys %RNASeqHash) {
    my $OrgToTransTo = $RNASeqHash{$test};
    if (($useOrthology eq "no" ) && ($OrgToTransTo ne $userOrganism)) {
	print STDERR "USE ORTHO SET TO NO and $OrgToTransTo doesnt match $userOrganism\n";
	next;


    }
    else {
	my $datasetToCheck = $test;
	my %rank = &createRankedList($dbh, $datasetToCheck);
	my $conList = $transformedListHash{$OrgToTransTo};
	my ($tempConFh, $tempConFile) = tempfile(SUFFIX => '.gmx');
	foreach my $ids (@$conList) {
#		print STDERR  "IDS ARE $ids \n";
	    print $tempConFh $ids."\n";
	}
#	die "KILLED IT";
	my ($tempRankFh, $tempRankFile) = tempfile(SUFFIX => '.rnk');
	my @keys = sort { $rank{$a} <=> $rank{$b} } keys(%rank);
	
	foreach my $element (@keys) {
#need to create a sorted file
#	print STDERR "element is $element\n";
#	print STDERR "temp file is $tempRankFile\n";
	    print $tempRankFh $element."\t".$rank{$element}."\n";
	}
	my $match = &runGSEA($tempRankFile,$tempConFile);
	my @results = @$match;
	my $fdrToCheck = $results[3];
	print STDERR "fdr to check is $fdrToCheck\n";
	if ($fdrToCheck <= $FDR) {

	    $matchedDatasets{$datasetToCheck}=$fdrToCheck;
	}
	else {
          next;
	}
    }
}
    


#results I want are probably the dataset that hits the threshold, the FDR, nominal pvalue - needs warning 

#so this is working with the %matchedDatasets 
foreach my $returnResult (keys %matchedDatasets) {
#    my $listResults = $matchedDatasets{$returnResult};
    print $returnResult."\t".$matchedDatasets{$returnResult}."\n";
}


# clean up
$dbh->disconnect();


# ----------------------------------------------------------------------
# Subroutines
# ----------------------------------------------------------------------

# ----------------------------------------------------------------------
sub transformByOrtholog {
#do something when there arnt any 
#there may be some issues for those outside our scope such as vectorbase - see how we did it ?!? 
    my ($dbh, $OrgToTransTo, $idSql) = @_;
#my $list = $userGeneList;
#    print STDERR "idsql is $idSql and ortho_organism is $OrgToTransTo\n";
    my $sql = <<EOSQL;
    
    select distinct og.ortho_source_id from 
	apidbTuning.OrthologousGenes og,
	($idSql) idsql  
	where idsql.gene_source_id = og.source_id 
	and ortho_organism = '$OrgToTransTo'
EOSQL
my $sth = $dbh->prepare($sql);
    
    $sth->execute();
    my $line;
    $sth->bind_columns(undef, \$line);
    
    my @List;
    
    while( $sth->fetch() ) {
	push @List, $line;   
#	print STDERR "LINE IS $line\n";
    }
#    print "I AM STUCK\n\n\n";
#    $sth->finish();
#    my ($tempTransFh, $tempTransFile) = tempfile();
#create a temp file for the list #
    foreach my $orth (@List) {
#	print STDERR "ORTH IS $orth \n";
#	print $tempTransFile $orth."\n";
    }
    return \@List;
    
}



sub createRankedList {
    my ($dbh, $datasetToCheck) = @_;
    
    my $sql = <<EOSQL;
    select distinct source_id || '\t' ||  fdiff_abs 
from  ApidbTuning.DatasetGeneList  
where dataset_name = '$datasetToCheck'
EOSQL
    
my $sth = $dbh->prepare($sql);
    
    $sth->execute();
    my $line;
    $sth->bind_columns(undef, \$line);
    
    my %List;
    
    while( $sth->fetch() ) {
	my @temps = split "\t", $line;
	my $sourceId = $temps[0];
	my $abs_FC = $temps[1];
	$List{$sourceId} = $abs_FC;
#	print STDERR "source id $sourceId absFC $abs_FC\n";
    }
    $sth->finish();
#    my ($tempRankFh, $tempRankFile) = tempfile();
#create a temp file for the list 
 
return %List;
}


sub runGSEA {
    my ($rank,$conList) = @_;
    #where will GSEA run from? I need to get this installed too.
    use File::Temp qw/ tempfile  tempdir/;
#    print "files are $rank and $conList\n";
    my $tempDir = tempdir();
    chdir $tempDir;
    my $cmd = "java -Duser.home=/var/www/Common/tmp/wdkStepAnalysisJobs/apicommdevn -cp /var/www/brunkb.plasmodb.org/GSEA/gsea2-2.2.4.jar -Xmx1080m  xtools.gsea.GseaPreranked -gmx $conList -rnk $rank  -zip_report false -gui false -norm meandiv -nperm 1000 -scoring_scheme weighted -make_sets false -plot_top_x 0 -rnd_seed timestamp -set_max 500 -set_min 5 -collapse false -out $tempDir";
#    print "here it is".$ENV{PWD};
#    my $testing = `cat $conList`;
#    print $testing."\n\n";
#    my $test2 = `cat $rank`;
#    print "rank is $test2\n";
    my ($EnrichmentScore,$NEnrichmentScore,$NomPValue,$FDRQValue,$FWERQval,$leadingEdge,$tag,$list,$signal);
#    print STDERR "command being used is $cmd\n\n";
#    system($cmd);
    system("$cmd > /dev/null 2>&1");
    opendir(DIR, $tempDir) || die("Cannot open $tempDir !\n");
# Get contents of directory
    my @dir_contents= readdir(DIR);
    closedir(DIR);
    foreach my $dir (sort(@dir_contents)) {
#	print STDERR "GETTING TO DIR $dir\n";
 	if ($dir =~/^my_analysis/) {
 	    opendir(DIR2, $dir) || die("Cannot open $dir !\n");
	    # Get contents of directory
 	    my @contents= readdir(DIR2);
 	    foreach my $file (sort(@contents)) {
		#	print "file/dir is $file\n";
		if ($file =~ /gsea_report_for_na_pos_.+xls$/) {
		    # here is where I get my results to feed back - so I want to read the file and then just send back the required info not the file. then my whole script will return jsut what I want and can be columns so should work with the existing java maybe.
 		    my $resultFile = $dir."/".$file;
 		    my $results = `grep -v "^NAME" $resultFile`;
		    
		    #               print $results."\nRESULTS";
 		    my @temps = split /\t/, $results;
 		    $EnrichmentScore = $temps[4];
		    #	    print "enrchiment is $EnrichmentScore\n";
 		    $NEnrichmentScore = $temps[5];
 		    $NomPValue = $temps[6];
 		    $FDRQValue = $temps[7];
 		    $FWERQval = $temps[8];
 		    $leadingEdge =$temps[10];
 		    if ($leadingEdge =~ m/tags=(\d+)%.+=(\d+)%.+=(\d+)%$/) {
 			$tag = $1;
 			$list = $2;
 			$signal = $3;
 		    }
 		}
 		else {
#		    print "getting to skip files\n";
		    next;
 		}
	    }
	}
	else {
#	    print "dir is $dir";
	}
    }
    #else {
    #    next;
    #}
    my @finalResults = ($EnrichmentScore,$NEnrichmentScore,$NomPValue,$FDRQValue,$FWERQval,$tag,$list,$signal);
	print STDERR  " RESULTS $EnrichmentScore\t$NEnrichmentScore\t$NomPValue\t$FDRQValue\t$FWERQval\t$tag\t$list\t$signal";
    return \@finalResults;
    
    
}

sub getValidGeneList {
    my ($dbh, $sql) = @_;
#    print STDERR $sql;
    my @genes;
    my $stmt = $dbh->prepare("$sql") or die(DBI::errstr);
    $stmt->execute() or die(DBI::errstr);
    
    
    my $geneStr;
    while ((my $mygene) = $stmt->fetchrow_array()) {
	push @genes, $mygene;
	
    }
    
    die "Got no genes\n" unless scalar @genes > 1;

#    return join(",", map { '"' . $_ . '"' } @genes);;
    return @genes;
}
