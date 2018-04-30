#!/usr/bin/perl
# file_name:clustering_bridge.pl
# Clustering procedure for long-read ChIA-PET
# Utilizes pairToPair function in Bedtools

use Graph::Undirected;
use List::Util qw(min max sum);
use List::MoreUtils qw/ uniq /;

# Usage:
if($#ARGV != 2){
	print "perl clustering_bridge.pl <INPUT_bedpe> <self-ligation cutoff> <OUTPUT>\n";
	exit;
}

$cutoff = $ARGV[1];
open(INPUT, "<$ARGV[0]") or die "Cannot open input file!\n";
open(TEMP, ">temp.bedpe") or die "Cannot open temp.bedpe file!\n";
open(TEMP2, ">self_ligation.bedpe") or die "Cannot open self-ligation file!\n";
while($line = <INPUT>){
	chomp $line;
	@elements = split(/\t/, $line);
	if($elements[0] ne $elements[3]){
		print TEMP "$line\n";
	}
	else{
		if(abs($elements[1]-$elements[4]) >= $cutoff){
			print TEMP "$line\n";
		}
		else{
			print TEMP2 "$line\n";
		}
	}
}

#system("pairToPair -is -rdn -slop 500 -a temp.bedpe -b temp.bedpe > temp.pairToPair");
