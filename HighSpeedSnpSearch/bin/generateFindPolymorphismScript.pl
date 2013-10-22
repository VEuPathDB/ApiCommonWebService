#!/usr/bin/perl
use strict;
my ($strainsDir, $final, $polymorphismThreshold, $unknownThreshold) = @ARGV;

die "usage: perl generateFindPolymorphismScript.pl strains_dir final_file polymorphism_threshold unknown_threshold\n" unless scalar(@ARGV) == 4;

if ($strainsDir =~ /(.+)\/$/) { $strainsDir = $1 }   # strip trailing /

# get list of files into array

my @mergeQueueOriginal;

opendir(my $dh, $strainsDir) || die "can't opendir $strainsDir: $!";
my $strainsCount = 0;
foreach my $strainFile (readdir($dh)) {
  if ($strainFile != /^\d+$/) {
    push(@mergeQueueOriginal, $strainFile);
    $strainsCount++;
  }
}
closedir $dh;

my @mergeQueue = sort {-s "$strainsDir/$b" <=> -s "$strainsDir/$a"} @mergeQueueOriginal;

my $fifoCursor = 0;

print "set -e\n";
print "set -x\n";
print "date\n";
print "cd $strainsDir\n";
print "mkfifo ";
for (my $i = 1; $i <= scalar(@mergeQueueOriginal); $i++) {
  print "fifo$i ";
}
print "\n";

while(1) {
  if (scalar(@mergeQueue) == 1) {
    my $allMerged = shift(@mergeQueue);
    print "findPolymorphic $allMerged referenceGenome.dat $strainsCount $polymorphismThreshold $unknownThreshold > $final &\n";
    last;
  } else {
    my $input1 = shift(@mergeQueue);
    my $input2 = shift(@mergeQueue);
    $fifoCursor++;
    push(@mergeQueue, "fifo$fifoCursor");
    my $strain1 = $input1 =~ /^\d+/? $input1 : 0;
    my $strain2 = $input2 =~ /^\d+/? $input2 : 0;
    print "mergeStrains $input1 $strain1 $input2 $strain2 > fifo$fifoCursor &\n";
  }
}
print "wait\n";
print "rm ";
for (my $i = 1; $i <= scalar(@mergeQueueOriginal); $i++) {
  print "fifo$i ";
}
print "\n";
print "date\n";
print "exit\n";
