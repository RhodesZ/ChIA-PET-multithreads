import pandas as pd
import sys

loop = pd.read_table("tmp_withLinker_clusters.txt",sep = "\t",header = None)

loop.columns = ["chromosome1",'start1','end1','chromosome2','start2','end2','pet']
chrom_list = ["chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", \
	"chr10", "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", \
	"chr18", "chr19", "chr20", "chr21", "chr22", "chrX"]
list1 = []

for i in xrange(len(loop)):
	if (i % 100 == 0) and (i != 0):
		print "%d\r" %(i),
		sys.stdout.flush()


	chromosome1 = loop["chromosome1"][i]
	chromosome2 = loop["chromosome2"][i]

	if ((chromosome1 in chrom_list) == False) or ((chromosome2 in chrom_list) == False):
		list1.append(i)


print len(loop)
loop = loop.drop(list1,axis = 0)
print len(loop)

loop=loop.sort_values(by = ["chromosome1",'start1','chromosome2','start2'])
loop.to_csv("loop_from_Ch.csv",index = False,sep = ",")