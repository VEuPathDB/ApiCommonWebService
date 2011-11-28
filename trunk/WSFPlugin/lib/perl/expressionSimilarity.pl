#!/usr/bin/perl
use strict;

use lib "$ENV{GUS_HOME}/lib/perl";

use DBI;
use DBD::Oracle;


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# Expression profiles with more than this number of datapoints missing
# will not be returned.
#
my $MAX_TOTAL_MISSING_DATAPOINTS = 4;

# NOTE:
# The above thresholds are a way of addressing the problem of comparing
# profiles that may have different numbers of non-null values.  It's a
# workaround for the fact that there's no easy way to compare (for example)
# a 3-dimensional distance or correlation with a 5-dimensional one.

# Subroutine and sort order to use for each distance measure
#
my $DISTANCE_MEASURES = {
    'euclidean_distance' => { sub => \&euclidean_distance, 
			      order => 'asc', 
			      name => 'Euclidean distance',
			      revName => 'Reverse Euclidean distance',
			      type => 'distance',
			      label => 'euclidean_distance',
			      },
    'pearson_correlation' => { sub => \&pearson_correlation, 
			       order => 'desc', 
			       name => 'Pearson correlation',
			       revName => 'Negative Pearson correlation',
			       type => 'correlation',
			       label => 'pearson_correlation',
			       },
};


# ----------------------------------------------------------------------
# Input
# ----------------------------------------------------------------------
# ARGV[0] - dist_method_param, choose from "euclidean_distance" or "pearson_correlation"
# ARGV[1] - num_to_return, choose from: 10, 25, 50, 100, 250, 500
# ARGV[2] - gene_id, example: "M56059_3"
# ARGV[3] - profileset, example: "DeRisi 3D7 Smoothed Averaged", "DeRisi HB3 Smoothed Averaged"
# ARGV[4] - search_goal, choose from "dissimilar" or "similar"
# ARGV[5] - time_shift, choose from -24 ~ 24
# ARGV[6] - time_shift_plus_minus, choose from 0 ~ 12
# ARGV[7] - # Number of time points in the data files  (48 for Plasmo DeRisi data, for example)
# ARGV[8] - # time points that are allowed to be skipped in the input ( has to be [23,29] for Plasmo Derisi data)
# ARGV[9] - db_connection
# ARGV[10] - db_login
# ARGV[11] - db_password

my $dist_method_param = $ARGV[0];
$dist_method_param =~ s/[^\_A-Za-z]//g;
my $dist_method = $DISTANCE_MEASURES->{$dist_method_param};

my $num_to_return = $ARGV[1];
$num_to_return =~ s/[^0-9]//g;

my $gene_id = $ARGV[2];

# get the profileset to query against
my $profileSet = $ARGV[3];

# whether to query for similar profiles or dissimilar profiles
my $searchGoal = ($ARGV[4] =~ /dissimilar/i) ? 'dissimilar' : 'similar';

# whether to do a time shift query i.e., look for profiles that are nearby/
# far away when timeshifted.
# keeping this variable, as it is needed for a hack (see below)
my $tShift = 1;

# minimum shift value to try
my $timeShift = $ARGV[5];
$timeShift =~ s/[^-0-9]//g;


# In Toxo M.White Cell Cycle data, each hour is broken into 5 parts;
# i.e. there are 12 hours * 5 = 60 data points. So, need scaleFactor param
my $scaleFactor = $ARGV[6];

# We would like profile to have at least 10 data points
# but there may be exceptions (for e.g. Crypto RT PCR dataset)
my $minPoints = $ARGV[7];

# Maximum fraction of the datapoints included in the query that can
# can be missing for a profile to be included in the result set.
#
my $MAX_QUERIED_MISSING_DATAPOINTS_FRAC = ($ARGV[8]/100);

##TO DO: REMOVE ALL REFS to $SKIP_TIMES;
my $SKIP_TIMES = [];


# read the db connection information
my $dbConnection = $ARGV[9];
my $dbLogin = $ARGV[10];
my $dbPassword = $ARGV[11];



# setup DBI connections
my $dbh = DBI->connect($dbConnection, $dbLogin, $dbPassword);

# get the expression profile of the given gene
my $sql = <<EOSQL;
SELECT profile_as_string
FROM apidb.profile pr, apidb.profileset ps
WHERE ps.profile_set_id = pr.profile_set_id
AND pr.source_id = '$gene_id'
AND ps.name like '$profileSet'
EOSQL

my $sth = $dbh->prepare($sql);

$sth->execute();
my $query_vector;
$sth->bind_columns(undef, \$query_vector);
$sth->fetch();
$sth->finish();

$query_vector =~ s/\s+/,/g;
$query_vector =~ s/[^,0-9\.-]//g;
my @queryArray = split(/,/, $query_vector);
my $numValues = @queryArray;

my $NUM_TIME_POINTS = scalar @queryArray;


my @weightArray;
my $ctValidPts = $NUM_TIME_POINTS; # $ctValidPts should be at least $minPoints
foreach my $wt (@queryArray) {
  if ($wt =~ /na/i) {
    push(@weightArray, 0);
    $ctValidPts--;
  } else {
    push(@weightArray, 1);
  }
}
my $numWeights = @weightArray;

my $inputValid = 1;
my $errorMsg;

if ( $ctValidPts < $minPoints){
    $errorMsg = ("ERROR: Input Gene ID does not have enough data points for this query \n" .
		 "Please try with another Gene ID.\n");
    $inputValid = 0;
}


# Modify parameters to look for dissimilar profiles 
#
if ($searchGoal =~ /dissimilar/i) {
    $dist_method->{order} = ($dist_method->{order} =~ /asc/i) ? 'desc' : 'asc';
    $dist_method->{name} = $dist_method->{revName};
}

$| = 1;

# Allow user to omit supplying values for the time points for
# which no data is available.
#
##TO DO:  REMOVE ALL REFS to $SKIP_TIMES;
my $numSkipTimes = scalar(@$SKIP_TIMES);
if (($numValues == $numWeights) && ($numValues == ($NUM_TIME_POINTS - $numSkipTimes))) {
    foreach my $st (@$SKIP_TIMES) {
	splice(@queryArray, $st-1, 0, '-');
	splice(@weightArray,$st-1, 0, 0);
	++$numValues;
	++$numWeights;
    }
}

# Check whether the query vector is constant (i.e., all its values are the same)
#
my $queryVectorConstant = 1;
my $first = $queryArray[0];

foreach my $qv (@queryArray) {
    if ($qv != $first) {
	$queryVectorConstant = 0;
	last;
    }
}

# Remove any values from the query vector that have weight == 0; this simplifies the display.
#
for(my $i = 0;$i < $numValues; ++$i) {
    if (($weightArray[$i] < 0) || ($weightArray[$i] eq '-')) { 
	$weightArray[$i] = 0;
    } elsif ($weightArray[$i] > 1) {
	$weightArray[$i] = 1;
    }

    if ($weightArray[$i] == 0) {
	$queryArray[$i] = '';
    }
}

# If the user has selected Pearson correlation and the query vector is 
# constant then display an error message; correlation is undefined in 
# this case.
#

if (($dist_method->{name} =~ /pearson correlation/i) && $queryVectorConstant) {
    $errorMsg = ("ERROR: Pearson correlation cannot be used if the query vector is constant, since the \n" .
		 "correlation is undefined in this case.  Please either change one or more of the \n" .
		 "query values or try using a different distance measure instead.\n");
    $inputValid = 0;
}
			     
# See if there are any values left (!)
#
my $haveValues = 0;
foreach my $value (@queryArray) {
    if ($value =~ /\d/) {
	$haveValues = 1;
	last;
    }
}

if (!$haveValues) {
    $errorMsg = ("ERROR: The query vector doesn't contain any values with nonzero weight.");
    $inputValid = 0;
}

if ($inputValid) {
    my $startTime = time;
    my $neighbors = undef;

    my $num_requested = $num_to_return;

    # HACK - if doing a time shift query, request some extra hits and then ignore 
    # those that are merely shifted by +1 or -1 from one we've already seen.
    #
    if ($tShift) {
	$num_requested *= 2;
    }

    # adjust the min and max time shifts
    my ($minShift, $maxShift);
    my $boolNegShift = 0; # to be set when shift is negative

    if ($timeShift < 0) {
      $minShift = $NUM_TIME_POINTS + $timeShift;
      $maxShift = $NUM_TIME_POINTS + $timeShift;
      $boolNegShift = 1;
    } else {
      $minShift = $timeShift;
      $maxShift = $timeShift;
    }

    if ($scaleFactor > 1) {
      $minShift *= $scaleFactor;
      $maxShift *= $scaleFactor;
    }

    $neighbors = &get_neighbors_perl($dbh, $dist_method, $num_requested, \@queryArray, \@weightArray, $profileSet, $minShift, $maxShift, $boolNegShift);

    foreach my $nbr (@$neighbors) {
	# print "\n<BR CLEAR=\"both\">\n" if ($hitnum > 1);

	my $elementId = $nbr->{elementId};
	my $dist = $nbr->{distance};
	my $shift = $nbr->{shift};
        
        print $elementId . "\t" . $dist ."\t" . $shift . "\n";
    }
} else {
    print STDERR "ERROR: Invalid input. Query failed.\n";
}

# clean up
$dbh->disconnect();


# ----------------------------------------------------------------------
# Subroutines
# ----------------------------------------------------------------------

# ----------------------------------------------------------------------
# get_neighbors_perl
#
# Greg Grant, Jonathan Crabtree
#
# dbh: the database connection handle
# distance_method: element of $DISTANCE_MEASURES
# number_to_return: the number of nearest neighbors to return
# query_vector_ref: (ref to) the vector we are looking for neighbors of
# w_ref: (ref to) the vector of weights
# profileSet: the name of the profile for genes
# min_shift: minimum shift amount; must be >= 0 and < $NUM_TIME_POINTS
# max_shift: maximum shift amount; must be >= 0 and < $NUM_TIME_POINTS
# ----------------------------------------------------------------------
sub get_neighbors_perl {
    my ($dbh, $distance_method, $number_to_return, $query_vector_ref, $w_ref, $profileset, $min_shift, $max_shift, $boolNegShift) = @_;

    my $distanceSub = $distance_method->{sub};
    my $sortOrder = $distance_method->{order};

    if (!defined($distanceSub)) {
	print STDERR "queryByExpressionProfile.pl: ERROR - unknown distance method '$distance_method->{name}'\n";
	return undef;
    }

    # Sum all weights - used in some of the distance measures
    # Also counts number of nonzero weights.
    my $n=@$w_ref;
    my $W=0;
    my $numNonzeroWeights = 0;
    for(my $i=0; $i<$n; $i++) {
        $W += $w_ref->[$i];
	++$numNonzeroWeights if ($w_ref->[$i] > 0);
    }

    my $vector_length = @$query_vector_ref;

   
    # Keep track of number of values that appear in each line
    my $minDataWidth = undef;
    my $maxDataWidth = undef;

    # Array of top $number_to_return hits found thus far, and their distances
    # The best hit and distance are always in array position 0
    my $bestHits = [];
    my $bestDistances = [];
    my $numHitsSoFar = 0;

    my $line;
    my $linenum = 0;
     
    # get the expression profile of the given gene
    my $sql = <<EOSQL;
SELECT source_id || '\t' || profile_as_string
FROM apidb.profile pr, apidb.profileset ps
WHERE ps.profile_set_id = pr.profile_set_id
AND ps.name like '$profileset'
EOSQL
    
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    $sth->bind_columns(undef, \$line);
    while( $sth->fetch() ) {
        my @c = split(/\t/,$line);
        my $m=@c;  # width of data

        if ($m > $maxDataWidth) {
            $maxDataWidth = $m;
        } elsif (($m < $minDataWidth) || (!defined($minDataWidth))) {
            $minDataWidth = $m;
        }

        # Extract spot/oligo ID for this array element
        my $eltId = shift @c;
        my @cd = map { chomp; $_; } @c;

        if (!($eltId =~ /\S/)) {
            print STDERR "queryByExpressionProfile.pl: ERROR - missing element_id at line '$line'\n";
        }

        # Determine whether this expression profile should be included in the result set
        my $numMissing = 0;        # absolute number of missing data points
        my $numQueryMissing = 0;   # number of data points missing from those with weight > 0

        for (my $i = 0;$i < $m;++$i) {
            my $val = $c[$i];
            if (!defined($val) || ($val =~ /^\s*$/) || ($val =~ /na/i)) {
                ++$numMissing;
                $cd[$i] = undef;
                if ($w_ref->[$i] > 0) {
                    ++$numQueryMissing;
                }
            }
        }

        ++$linenum;

	    # Skip this profile if it is missing more than a specified number of datapoints
	    next if ($numMissing > ($NUM_TIME_POINTS - $minPoints + 1));

	    # Skip this profile if more than a specified fraction of the datapoints being queried
	    # for (i.e., those with nonzero weights) are missing
	    my $fracMissing = $numQueryMissing/$numNonzeroWeights;
	    next if ($fracMissing > $MAX_QUERIED_MISSING_DATAPOINTS_FRAC);

	    # Range of time shifts to apply to the data
	    my $startShift = 0;
	    my $endShift = 0;

            if (($min_shift =~ /\d/) && ($min_shift >= 0) && ($min_shift < ($NUM_TIME_POINTS * $scaleFactor))) {
                $startShift = $min_shift;
            }
            if (($max_shift =~ /\d/) && ($max_shift >= 0) && ($max_shift < ($NUM_TIME_POINTS * $scaleFactor)) && ($max_shift >= $min_shift)) {
                $endShift = $max_shift;
            } else {
                $endShift = $NUM_TIME_POINTS - 1;  # i.e., try them all
            }

	    # Compute and store distance for this row/element, for each time shift
	    my $bestDist = &$distanceSub($query_vector_ref, \@cd, $startShift, $w_ref, $W);
	    my $bestShift = $startShift;
        for (my $shiftAmount = $startShift+1;$shiftAmount <= $endShift;++$shiftAmount) {
            my $dist = &$distanceSub($query_vector_ref, \@cd, $shiftAmount, $w_ref, $W);
            
            # for each gene, only keep the best score of it
            if ((($sortOrder eq 'asc') && ($bestDist > $dist)) 
                    || (($sortOrder eq 'desc') && ($bestDist < $dist))) {
                $bestDist = $dist;
                $bestShift = $shiftAmount;
            }
        }
        
        # Find out whether (and where) this distance belongs in $bestDistances
        my $arrayInd = &binarySearch($bestDistances, $bestDist, $sortOrder);

        # It belongs among the top hits found so far
        if ($arrayInd < $number_to_return) {
	  # adjust shift for $scaleFactor (needed for: Toxo's Cell Cycle)
	  if ($scaleFactor > 1) {
	    $bestShift = int($bestShift / $scaleFactor);
	    }

	    $bestShift = $bestShift - $NUM_TIME_POINTS if ($boolNegShift == 1);
            my $hit = { elementId => $eltId, shift => $bestShift, distance => $bestDist };

            splice(@$bestDistances,$arrayInd,0,$bestDist);
            splice(@$bestHits,$arrayInd,0,$hit);

            # It's replacing an existing entry; must remove the extra array element
            if ($numHitsSoFar == $num_to_return) {
                splice(@$bestDistances,$numHitsSoFar,1);
                splice(@$bestHits,$numHitsSoFar,1);
            } 

            # Otherwise it's a new entry.
            else 
            {
                ++$numHitsSoFar;
            }
        }
    }
    $sth->finish();

    # Print an error if any of the rows of data have different width from query vector
    #if ((($minDataWidth-1) != $vector_length) || (($maxDataWidth-1) != $vector_length)) {
        #print STDERR "queryByExpressionProfile.pl: WARNING - source has minWidth=$minDataWidth, ",
        #    "maxWidth=$maxDataWidth, but query vector has length=$vector_length\n";
        #print "-->$query_vector\n"; 
    #}

    # JC: one problem with arbitrarily restricting to first N hits is that you
    # might be excluding from the output profiles that have the same distance as
    # profiles that were included

    return $bestHits;
}


# ----------------------------------------------------------------------
# Sorting and searching
# ----------------------------------------------------------------------

sub binarySearch {
    my($arr, $x, $sortOrder) = @_;

    my $min = 0; 
    my $max = scalar(@$arr) - 1;
    my $m = undef;

#    print STDERR "queryByExpressionProfile.pl: searching for $x in [", join(',', @$arr) , "]\n";
    
    if ($sortOrder eq 'asc') {
	while (1) {
	    if ($max < $min) {
		if ($x > $arr->[$m]) {
		    return $m + 1;
		} else {
		    return $m;
		}
	    }

	    $m = int(($min + $max) / 2);

#	    print STDERR "queryByExpressionProfile.pl: min=$min max=$max m=$m comparing to ", $arr->[$m], "\n";
	    
	    if ($arr->[$m] < $x) {
		$min = $m + 1;
	    } elsif ($arr->[$m] > $x) {
		$max = $m - 1;
	    } else {
		return $m;
	    }
	}
    } else {
	while (1) {
	    return $min if ($max < $min);
	    $m = int(($min + $max) / 2);
	    
	    if ($arr->[$m] > $x) {
		$min = $m + 1;
	    } elsif ($arr->[$m] < $x) {
		$max = $m - 1;
	    } else {
		return $m;
	    }
	}
    }
}


# ----------------------------------------------------------------------
# Subroutines for the various distance measures
# ----------------------------------------------------------------------

# $offset - offset into $dataVector to consider the start of the array
#
sub euclidean_distance {
    my($queryVector, $dataVector, $offset, $weights, $W) = @_;
    my $vector_length = scalar(@$queryVector);
    my $sum = 0;

    for (my $i=0; $i<$vector_length; $i++) {
	my $dataInd = ($i + $offset) % $vector_length;
	my $val = $dataVector->[$dataInd];

	# Don't include missing values in distance calculation
	if ($val =~ /\S/) {
	    $sum += $weights->[$i] * ($val - $queryVector->[$i])**2;
	}
    }

    return sqrt($sum);
}

# $offset - offset into $dataVector to consider the start of the array
#
sub pearson_correlation {
    my($queryVector, $dataVector, $offset, $weights, $W) = @_;
    my $vector_length = scalar(@$queryVector);

    my $xy=0;
    my $x=0;
    my $y=0;
    my $xx=0;
    my $yy=0;

    for (my $i=0; $i<$vector_length; $i++) {
	my $dataInd = ($i + $offset) % $vector_length;
	$xy += $weights->[$i] * $dataVector->[$dataInd] * $queryVector->[$i];
	$x += $weights->[$i] * $dataVector->[$dataInd];
	$y += $weights->[$i] * $queryVector->[$i];
	$xx += $weights->[$i] * ($dataVector->[$dataInd])**2;
	$yy += $weights->[$i] * ($queryVector->[$i])**2;
    }

    return ($W * $xy - $x*$y)/(sqrt(($W*$xx-$x*$x)*($W*$yy-$y*$y)));
}
