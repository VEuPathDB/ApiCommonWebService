#!/usr/bin/perl                                                                                                                     
use strict;
use lib "$ENV{GUS_HOME}/lib/perl";
use DBI;
use DBD::Oracle;
use File::Temp qw/ tempfile /;
use Data::Dumper;


my $idSql = $ARGV[0];
my $useOrthology = $ARGV[1];
$useOrthology =~ s/\'//g;
my $FC = $ARGV[2];

# read the db connection information
my $dbConnection = $ARGV[3];
my $dbLogin = $ARGV[4];
my $dbPassword = $ARGV[5];

# setup DBI connections
my $dbh = DBI->connect($dbConnection, $dbLogin, $dbPassword) or die "Unable to connect: DBI->errstr\n";



########################################################## userlist hash table ##################################################

my $idT = (lc($useOrthology) eq "yes") ? "orthomcl_name" : "source_id";

my $userGeneListQuery = "select distinct ga." . $idT . ", ga.organism from apidbtuning.geneattributes ga, (" . $idSql . ") id where id.gene_source_id = ga.source_id";

#print STDERR $userGeneListQuery . "\n";

my $userStatemnetHandle = $dbh->prepare($userGeneListQuery);

$userStatemnetHandle->execute();

my %userLists;


while(my ($id,$org) = $userStatemnetHandle->fetchrow_array() ) {

    $userLists{$org}->{$id}++;
}

$userStatemnetHandle->finish();

#print STDERR Dumper \%userLists;


########################################################## dataset list hash table ###############################################

my $datasetGeneListQuery = "select distinct ga." . $idT . ", ga.organism, ga.dataset_presenter_id from apidbtuning.datasetgenelist ga where ga.fdiff_abs > $FC";

my $datasetStatmentHandle = $dbh->prepare($datasetGeneListQuery);                                                                   
$datasetStatmentHandle->execute();
my %datasetLists;                                                                                                                  

while(my($id, $org, $dataset) = $datasetStatmentHandle->fetchrow_array() ) {                                             

    $datasetLists{$org}->{$dataset}->{$id}++;                                                                                       
}                                                                                                                                  $datasetStatmentHandle->finish();  

#print STDERR Dumper \%datasetLists;


######################################################### combined calculation ### ###############################################


###################################### give each of those columns a colname

print "dataset_id", "\t", "overlap","\t", "ul_nonDS","\t", "ds_nonUL". "\t", "nonUL_nonDS", "\t", "p_value", "\n";

foreach my $org (keys %userLists){
    my $backgroundSize = &getBackgroundForOrganism($dbh, $org, $idT); ########## background SIZE                       
    my @userIdList = keys %{$userLists{$org}}; 
    my $userListSize = scalar @userIdList;   ############################### user_list SIZE

    # make a hash from the userIdList Array
    my %userIdListHash = map { $_ => 1 } @userIdList;

    foreach my $dataset (keys %{$datasetLists{$org}}){
	my  @datasetIdList = keys %{$datasetLists{$org}->{$dataset}};

	my $datasetListSize  =  scalar @datasetIdList; ####################### dataset_list SIZE
	
	my $t11; my $t21; my $t12; my $t22;
       
	my ($FF, $TXT) = tempfile(SUFFIX => '.txt');

	# Find the number of overlap between two lists
	foreach my $value1 (@datasetIdList){
          if($userIdListHash{$value1}) {
            $t11++;
          }
	}

	$t12 = $datasetListSize - $t11;
	$t21 = $userListSize - $t11;
	$t22 = $backgroundSize - $t11 - $t12 -$t21;
	my $pValue = &runRscript($t11, $t21, $t12, $t22);

# NOTE: if we need to restrict by pvalue, we should read this as a param, not use 0.1 hard coded	
#	if ($pValue < 0.1){
            print  $dataset,"\t",$t11,"\t",$t21, "\t", $t12, "\t", $t22, "\t", $pValue, "\n";
#	}

    }
}

sub runRscript{
    my ($t11,$t21,$t12,$t22) = @_;
    
    my $rCode = <<"RCODE";

    Test <-matrix(c($t11,$t21,$t12,$t22),nrow = 2, dimnames = list(DS = c("DS", "Non-DS"),UL = c("UL", "Non-UL")))
    fisher.test(Test, alternative = "greater")\$p

RCODE

    my ($FH, $file) = tempfile(SUFFIX => '.R');     
    
    print $FH  $rCode;

    my $command = "Rscript " .  $file;
      
    my $p_value  =  `$command`;
    
    close (FH);
    
    if ($p_value =~ m/^\[1\]\s*([\d]+.+)/) {
        #print "$1\n";
	return  $1;
    }
}


sub getBackgroundForOrganism{
    my ($dbh, $org, $idType) = @_;

    my $sql = <<EOSQL;
     select count(distinct $idType) 
     from  APIDBTUNING.GENEATTRIBUTES  
     where organism = '$org'
EOSQL
    
    my $sth = $dbh->prepare($sql);
    $sth->execute();

    my $idCount = $sth->fetchrow_array();
    
    return $idCount;
}



1;
