
use Graph::Undirected;
use List::Util qw(min max sum);
use List::MoreUtils qw/ uniq /;

$graph = Graph::Undirected->new;
open(PAIR, "<$ARGV[0]") or die "Cannot open temp.pairToPair file!\n";
while($line = <PAIR>){
	chomp $line;
	@elements = split(/\t/, $line);
	$uid1 = $elements[7];
	$uid2 = $elements[15];

	$uid1_head = join(':', $elements[0], $elements[1], $elements[2]);
	$uid1_tail = join(':', $elements[3], $elements[4], $elements[5]);
	$uid2_head = join(':', $elements[8], $elements[9], $elements[10]);
	$uid2_tail = join(':', $elements[11], $elements[12], $elements[13]);

	$hash{$uid1}{'head'} = $uid1_head;
	$hash{$uid1}{'tail'} = $uid1_tail;
	$hash{$uid2}{'head'} = $uid2_head;
	$hash{$uid2}{'tail'} = $uid2_tail;

	$graph->add_edge($uid1, $uid2);

}
@cc = $graph->connected_components();

open(OUTPUT, ">$ARGV[1]") or die "Cannot open output file\n!";
for($k=0; $k<=$#cc; $k++){
	@members = @{$cc[$k]};
	$cluster_size = $#members + 1;

	@cluster_head_chr = ();
	@cluster_head_start = ();
	@cluster_head_end = ();
	@cluster_tail_chr = ();
	@cluster_tail_start = ();
	@cluster_tail_end = ();
	foreach $m (@members){
		($head_chr, $head_start, $head_end) = split(':', $hash{$m}{'head'});
		($tail_chr, $tail_start, $tail_end) = split(':', $hash{$m}{'tail'});
		push @cluster_head_chr, $head_chr;
		push @cluster_head_start, $head_start;
		push @cluster_head_end, $head_end;
		push @cluster_tail_chr, $tail_chr;
		push @cluster_tail_start, $tail_start;
		push @cluster_tail_end, $tail_end;
	}
	@cluster_head_chr_uniq = uniq(@cluster_head_chr);
	$cluster_head_start_site = min(@cluster_head_start);
	$cluster_head_end_site = max(@cluster_head_end);
	@cluster_tail_chr_uniq = uniq(@cluster_tail_chr);
	$cluster_tail_start_site = min(@cluster_tail_start);
	$cluster_tail_end_site = max(@cluster_tail_end);
	print OUTPUT "@cluster_head_chr_uniq\t$cluster_head_start_site\t$cluster_head_end_site\t@cluster_tail_chr_uniq\t$cluster_tail_start_site\t$cluster_tail_end_site\t$cluster_size\n";
}

close INPUT, OUTPUT;