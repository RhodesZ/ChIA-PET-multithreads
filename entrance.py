import os
import multiprocessing
import time
import sys
def single(name):
	fastaq = open("%s" %(name),"r")
	linker = open("%s_withLinker_info" %(name),"r")
	head = open("%s_withLinker_head" %(name),"w")
	tail = open("%s_withLinker_tail" %(name),"w")
	linker_pos = {}


	line = linker.readline()
	while line:
		line = line.strip()
		info = line.split("\t")
		uid = "@" + info[0]
		linker_pos[uid] = [int(info[2]),int(info[3])]
		line = linker.readline()

	linker.close()

	flag = False
	line = fastaq.readline()
	count = 0
	while line:
		line = line.strip()
		#uid
		if count % 400000 == 0:
			print "Processed %d record\t%s\r" %(count / 4,name),
			sys.stdout.flush()
		if (count % 4)== 0:
			try:
				uid = line
				pos = linker_pos[line]
				flag = True
			except:
				flag = False
		elif (count % 4) == 1:
			sequence = line
		elif (count % 4) == 2:
			sep = 0
		elif (count % 4) == 3:
			quality = line
			if flag == True:
				flag = False
				before_linker_sequence = sequence[0:pos[0]]
				before_linker_quality = quality[0:pos[0]]
				after_linker_sequence = sequence[pos[1]:]
				after_linker_quality = quality[pos[1]:]
				head.write("%s\n" %(uid))
				head.write("%s\n+\n" %(before_linker_sequence))
				head.write("%s\n" %(before_linker_quality))

				tail.write("%s\n" %(uid))
				tail.write("%s\n+\n" %(after_linker_sequence))
				tail.write("%s\n" %(after_linker_quality))


		count += 1
		line = fastaq.readline()

	head.close()
	tail.close()
	fastaq.close()


def single_new(name):
	fastaq = open("%s" %(name),"r")
	linker = open("%s_withLinker_info" %(name),"r")
	head = open("%s_withLinker_head" %(name),"w")
	tail = open("%s_withLinker_tail" %(name),"w")
	
	hash_sequence = {}
	hash_quality = {}

	count = 0
	line = fastaq.readline()
	while line:
		line = line.strip()
		#uid
		if count % 400000 == 0:
			print "Processed %d record\t%s\r" %(count / 4,name),
			sys.stdout.flush()

		if (count % 4)== 0:
			uid  = line
		elif (count % 4) == 1:
			sequence = line
			hash_sequence[uid] = line
		elif (count % 4) == 2:
			sep = 0
		elif (count % 4) == 3:
			quality = line
			hash_quality[uid] = line

		count += 1
		line = fastaq.readline()

	fastaq.close()


	line = linker.readline()
	while line:
		line = line.strip()
		info = line.split("\t")
		uid = "@" + info[0]
		
		linker_start = int(info[2])
		linker_end = int(info[3])

		sequence = hash_sequence[uid]
		quality = hash_quality[uid]

		before_linker_sequence = sequence[0:linker_start]
		before_linker_quality = quality[0:linker_start]
		after_linker_sequence = sequence[linker_end:]
		after_linker_quality = quality[linker_end:]
		head.write("%s\n" %(uid))
		head.write("%s\n+\n" %(before_linker_sequence))
		head.write("%s\n" %(before_linker_quality))

		tail.write("%s\n" %(uid))
		tail.write("%s\n+\n" %(after_linker_sequence))
		tail.write("%s\n" %(after_linker_quality))


		line = linker.readline()

	linker.close()
	head.close()
	tail.close()


def readfastaq(filename):
	file = open(filename,"r")
	line = file.readline()
	sequence_hash = {}
	quality_hash = {}
	count = 0
	while line:
		line = line.strip()
		if count % 400000 == 0:
			print "%s:Processed %d record\r" %(filename,count / 4),
			sys.stdout.flush()
		if (count % 4)== 0:
			uid = line.split(" ")[0]
		elif (count % 4)== 1:
			sequence = line
			sequence_hash[uid] = sequence
		#Nothing to do here
		#elif (count % 4)== 2:
			#sep = 0
		elif (count % 4)== 3:
			quality = line
			quality_hash[uid] = quality
		count += 1
		line = file.readline()
	print 

	return sequence_hash,quality_hash

def pair(name):
	R1_total,R1_total_Q = readfastaq("tmp_R1_"+name)
	R1_head,R1_head_Q = readfastaq("tmp_R1_"+name+"_withLinker_head")
	R1_tail,R1_tail_Q = readfastaq("tmp_R1_"+name+"_withLinker_tail")

	R2_total,R2_total_Q = readfastaq("tmp_R2_"+name)
	R2_head,R2_head_Q = readfastaq("tmp_R2_"+name+"_withLinker_head")
	R2_tail,R2_tail_Q = readfastaq("tmp_R2_"+name+"_withLinker_tail")

	R1_WL = open("tmp_R1_"+name+"_withLinker_fastq","w")
	R2_WL = open("tmp_R2_"+name+"_withLinker_fastq","w")
	uids = list(R1_total.keys())
	uids.sort()

	for uid in uids:
		try:
			r1_head = R1_head[uid]
			r1_head_q = R1_head_Q[uid]
		except:
			r1_head = ""

		try:
			r1_tail = R1_tail[uid]
			r1_tail_q = R1_tail_Q[uid]
		except:
			r1_tail = ""
		r1_total = R1_total[uid]
		r1_total_q = R1_total_Q[uid]
		
		try:
			r2_head = R2_head[uid]
			r2_head_q = R2_head_Q[uid]
		except:
			r2_head = ""

		try:
			r2_tail = R2_tail[uid]
			r2_tail_q = R2_tail_Q[uid]
		except:
			r2_tail = ""
		r2_total = R2_total[uid]
		r2_total_q = R2_total_Q[uid]


		l1 = len(r1_head)
		l2 = len(r1_tail)
		l3 = len(r2_head)
		l4 = len(r2_tail)

		if((l1==0) & (l2==0) & (l3==0) & (l4==0)):
			#R1_NL.write("%s\n%s\n+\n%s\n" %(uid,r1_total,r1_total_q))
			#R2_NL.write("%s\n%s\n+\n%s\n" %(uid,r2_total,r2_total_q))
			nothingtodo = 0
		else:
			if((l1==0) & (l2!=0) & (l3==0) & (l4==0)):
				R1_WL.write("%s\n%s\n+\n%s\n" %(uid,r1_tail,r1_tail_q))
				R2_WL.write("%s\n%s\n+\n%s\n" %(uid,r2_total,r2_total_q))
			# Case 2b: linker at 3 of R1
			elif ((l1!=0) & (l2==0) & (l3==0) & (l4==0)):
				R1_WL.write("%s\n%s\n+\n%s\n" %(uid,r1_head,r1_head_q))
				R2_WL.write("%s\n%s\n+\n%s\n" %(uid,r2_total,r2_total_q))
			# Case 2c: linker in the middle of R1
			elif ((l1!=0) & (l2!=0) & (l3==0) & (l4==0)):
				if(l1<18) & (l2>=18):
					R1_WL.write("%s\n%s\n+\n%s\n" %(uid,r1_tail,r1_tail_q))
					R2_WL.write("%s\n%s\n+\n%s\n" %(uid,r2_total,r2_total_q))		
				else:
					R1_WL.write("%s\n%s\n+\n%s\n" %(uid,r1_head,r1_head_q))
					R2_WL.write("%s\n%s\n+\n%s\n" %(uid,r2_total,r2_total_q))	  	
			
			# Case 3: linker only in R2 but not R1
			# Case 3a: linker at 5 of R2
			if((l1==0) & (l2==0) & (l3==0) & (l4!=0)):
				R1_WL.write("%s\n%s\n+\n%s\n" %(uid,r1_total,r1_total_q))
				R2_WL.write("%s\n%s\n+\n%s\n" %(uid,r2_tail,r2_tail_q))
			# Case 3b: linker at 3 of R2
			elif ((l1==0) & (l2==0) & (l3!=0) & (l4==0)):
				R1_WL.write("%s\n%s\n+\n%s\n" %(uid,r1_total,r1_total_q))
				R2_WL.write("%s\n%s\n+\n%s\n" %(uid,r2_head,r2_head_q))
			
			# Case 3c: linker in the middle of R2
			elif ((l1==0) & (l2==0) & (l3!=0) & (l4!=0)):
				if(l3<18) & (l4>=18):
					R1_WL.write("%s\n%s\n+\n%s\n" %(uid,r1_total,r1_total_q))
					R2_WL.write("%s\n%s\n+\n%s\n" %(uid,r2_tail,r2_tail_q))			
				else:
					R1_WL.write("%s\n%s\n+\n%s\n" %(uid,r1_total,r1_total_q))
					R2_WL.write("%s\n%s\n+\n%s\n" %(uid,r2_head,r2_head_q))			
			
			
			# Case 4: linker in both R1 and R2
			# Linker in the middle
			if(\
				((l1!=0) & (l2!=0) & (l3!=0) & (l4!=0)) | \
				((l1==0) & (l2!=0) & (l3!=0) & (l4!=0)) | \
				((l1!=0) & (l2==0) & (l3!=0) & (l4!=0)) | \
				((l1!=0) & (l2!=0) & (l3==0) & (l4!=0)) | \
				((l1!=0) & (l2!=0) & (l3!=0) & (l4==0)) | \
				((l1!=0) & (l2==0) & (l3!=0) & (l4==0))):
				if(l1 >= l4):
					R1_WL.write("%s\n%s\n+\n%s\n" %(uid,r1_head,r1_head_q))
				else:
					R1_WL.write("%s\n%s\n+\n%s\n" %(uid,r2_tail,r2_tail_q))		
				
				if(l2 >= l3):
					R2_WL.write("%s\n%s\n+\n%s\n" %(uid,r1_tail,r1_tail_q))
				
				else:
					R2_WL.write("%s\n%s\n+\n%s\n" %(uid,r2_head,r2_head_q))
				
			
			# Linker at 5 end
			elif ((l1==0) & (l2!=0) & (l3!=0) & (l4==0)):
				R1_WL.write("%s\n%s\n+\n%s\n" %(uid,r1_tail,r1_tail_q))			
				R2_WL.write("%s\n%s\n+\n%s\n" %(uid,r2_head,r2_head_q))
			
			# Linker at 3 end
			elif ((l1!=0) & (l2==0) & (l3==0) & (l4!=0)):
				R1_WL.write("%s\n%s\n+\n%s\n" %(uid,r1_head,r1_head_q))			
				R2_WL.write("%s\n%s\n+\n%s\n" %(uid,r2_tail,r2_tail_q))
			
			# The other cases should be chimeric reads
			# no need...
			#elif ((l1==0) & (l2!=0) & (l3==0) & (l4!=0)):
				
	R1_WL.close()
	R2_WL.close()

def bwa(name):
	if os.path.isfile("./tmp_%s_withLinker_sam" %(name)):
		return
	os.system("bwa mem -t 16 -k 18 -M ../../GATK_hg19/ucsc.hg19.fasta tmp_R1_%s_withLinker_fastq tmp_R2_%s_withLinker_fastq > tmp_%s_withLinker_sam" %(name,name,name))

def pairtopair(name):
	os.system("pairToPair -is -rdn -slop 500 -a temp.bedpe -b temp_split_%s > temp_%s.pairToPair" %(name,name))

#Make change here! 12 - 18
def cutadapt_R1(name):
	filename = "tmp_R1_"+name
	os.system("cutadapt -f fastq -b ACGCGATATCTTATCTGACT -b AGTCAGATAAGATATCGCGT --info-file %s_withLinker_info --discard -O 18 %s --output %s_noLinker > %s_stat" %(filename,filename,filename,filename))

def cutadapt_R2(name):	
	filename = "tmp_R2_"+name
	os.system("cutadapt -f fastq -b ACGCGATATCTTATCTGACT -b AGTCAGATAAGATATCGCGT --info-file %s_withLinker_info --discard -O 18 %s --output %s_noLinker > %s_stat" %(filename,filename,filename,filename))

def remove_temp(filename):
	try:
		os.remove(filename)
	except:
		print "%s not exist" %(filename)

def clear_temp(list_temp):
	for name in list_temp:
		remove_temp("tmp_R1_%s" %(name))
		remove_temp("tmp_R1_%s_withLinker_info" %(name))
		remove_temp("tmp_R1_%s_withLinker_head" %(name))
		remove_temp("tmp_R1_%s_withLinker_tail" %(name))
		remove_temp("tmp_R1_%s_withLinker_fastq" %(name))

		remove_temp("tmp_R2_%s" %(name))
		remove_temp("tmp_R2_%s_withLinker_info" %(name))
		remove_temp("tmp_R2_%s_withLinker_head" %(name))
		remove_temp("tmp_R2_%s_withLinker_tail" %(name))
		remove_temp("tmp_R2_%s_withLinker_fastq" %(name))

		remove_temp("tmp_%s_withLinker_sam" %(name))
		remove_temp("tmp_%s_withLinker_bam" %(name))
		remove_temp("tmp_R1_%s_noLinker" %(name))
		remove_temp("tmp_R2_%s_noLinker" %(name))

def split_R1():
	os.system("split -l 120000000 tmp_R1.fastq tmp_R1_")
def split_R2():
	os.system("split -l 120000000 tmp_R2.fastq tmp_R2_")

def S2b(name):
	os.system("samtools view -Sb tmp_%s_withLinker_sam > tmp_%s_withLinker_bam" %(name,name))

def gettemp(rootpath,keyword,length):
	list_temp = []
	for (dirpath, dirnames, filenames) in os.walk(rootpath):
		for filename in filenames:
			if keyword in filename and len(filename) == length:
				list_temp.append(filename[-2:])
	list_temp.sort()
	print "Splitted file list"
	print list_temp
	return list_temp

def Start_splitting():
	print "Start Splitting"
	if os.path.isfile("./tmp_R1_aa"):
		print "Skipping splitting"
		return
	task_list = []
	t = multiprocessing.Process(target = split_R1)
	t.start()
	task_list.append(t)
	t = multiprocessing.Process(target = split_R2)
	t.start()
	task_list.append(t)

	for t in task_list:
		t.join()
	time.sleep(5)

def Start_cutadapt(list_temp,threads = 6):
	print "Start cutadapt"
	task_list = []
	for name in list_temp:

		if os.path.isfile("./tmp_R1_%s_noLinker" %(name)):
			print "Already done\r",
			sys.stdout.flush()
		else:
			t = multiprocessing.Process(target = cutadapt_R1,args = (name,))
			t.start()
			task_list.append(t)

		if os.path.isfile("./tmp_R2_%s_noLinker" %(name)):
			print "Already done\r",
			sys.stdout.flush()
		else:
			t = multiprocessing.Process(target = cutadapt_R2,args = (name,))
			t.start()
			task_list.append(t)
		if len(task_list) == threads:
			for t in task_list:
				t.join()
			time.sleep(5)
			task_list = []

	for t in task_list:
		t.join()
	time.sleep(5)

def Start_single(list_temp,threads = 4):
	print "Start Prepare single"
	task_list = []

	for name in list_temp:
		if os.path.isfile("./tmp_R1_%s_withLinker_head" %(name)):
			print "Already done\r",
			sys.stdout.flush()
		else:
			t = multiprocessing.Process(target = single_new,args = ("tmp_R1_%s" %(name),))
			t.start()
			task_list.append(t)

		if os.path.isfile("./tmp_R2_%s_withLinker_head" %(name)):
			print "Already done\r",
			sys.stdout.flush()
		else:
			t = multiprocessing.Process(target = single_new,args = ("tmp_R2_%s" %(name),))
			t.start()
			task_list.append(t)


		if len(task_list) == threads:
			for t in task_list:
				t.join()
			time.sleep(5)
			task_list = []
	for t in task_list:
		t.join()
	time.sleep(5)

def Start_pair(list_temp,threads = 2):
	print "Start Prepare pair"
	task_list = []
	for name in list_temp:
		if os.path.isfile("./tmp_R1_%s_withLinker_fastq" %(name)):
			print "Already done\r",
			sys.stdout.flush()
		else:
			t = multiprocessing.Process(target = pair,args = (name,))
			t.start()
			task_list.append(t)


		if len(task_list) == threads:
			for t in task_list:
				t.join()
			time.sleep(5)
			print "\nFinish %d pair" %(threads)
			task_list = []
	for t in task_list:
		t.join()
	time.sleep(5)

def Start_S2b(list_temp,threads = 5):
	task_list = []
	for name in list_temp:
		t = multiprocessing.Process(target = S2b,args = (name,))
		t.start()
		task_list.append(t)


		if len(task_list) == threads:
			for t in task_list:
				t.join()
			time.sleep(5)
			print "\nFinish 5"
			task_list = []
	for t in task_list:
		t.join()
	time.sleep(5)

def Start_bwa(list_temp,threads = 2):
	task_list = []
	for name in list_temp:
		if os.path.isfile("./tmp_%s_withLinker_sam" %(name)):
			continue
		t = multiprocessing.Process(target = bwa,args = (name,))
		t.start()
		task_list.append(t)


		if len(task_list) == threads:
			for t in task_list:
				t.join()
			time.sleep(5)
			print "\nFinish 5"
			task_list = []
	for t in task_list:
		t.join()
	time.sleep(5)

def split_chr():
	result_file = open("chr/temp_inter.pairToPair","w")
	result_dict = {}
	result_dict['inter'] = result_file


	file = open("temp.pairToPair","r")
	line = file.readline()
	uidlist1 = []
	uidlist2 = []
	count = 0
	while line:
		count += 1
		if count % 10000 == 0:
			print "%d\r" %(count),
			sys.stdout.flush()
		line = line.strip()
		elements = line.split("\t")

		head_chr = elements[0]
		tail_chr = elements[3]


		if head_chr != tail_chr:
			result_file = result_dict["inter"]
		else:
			if head_chr in result_dict.keys():
				result_file = result_dict[head_chr]
			else:
				result_file = open("chr/temp_%s.pairToPair" %(head_chr),"w")
				result_dict[head_chr] = result_file

		result_file.write(line+"\n")
		
		line = file.readline()
	for file in result_dict.values():
		file.close()

def cluster(name,count):
	os.system("perl ../../clustering_bridge2.pl ./chr/%s tmp_withLinker_clusters_%d.txt" %(name,count))
'''
Start_splitting()
list_temp = gettemp("./","tmp_R1_",9)
Start_cutadapt(list_temp,8)
Start_single(list_temp,6)
Start_pair(list_temp,2)
Start_bwa(list_temp,4)
Start_S2b(list_temp,10)


command = "samtools cat -o tmp_withLinker_bam "
for name in list_temp:
	command += "tmp_%s_withLinker_bam " %(name)
os.system(command)
os.system("samtools sort tmp_withLinker_bam > tmp_withLinker_sorted")
os.system("java -jar ../../picard.jar MarkDuplicates I=tmp_withLinker_sorted O=tmp_withLinker_pcd.bam M=tmp_withLinker_pcd.matrix REMOVE_DUPLICATES=true")
os.system("samtools view -h -q 30 -F 256 -b tmp_withLinker_pcd.bam -b -o tmp_withLinker_pcd_uniq.bam")
os.system("samtools sort -n  tmp_withLinker_pcd_uniq.bam > tmp_withLinker_sortByname")
os.system("bedtools bamtobed -bedpe -i tmp_withLinker_sortByname > tmp_withLinker_bedpe.txt")
os.system("perl ../../clustering_bridge.pl tmp_withLinker_bedpe.txt 8000 tmp_withLinker_clusters.txt")
os.system("split -l 1000000 temp.bedpe temp_split_")
clear_temp(list_temp)
list_temp = gettemp("./","temp_split_",13)
print list_temp
task_list = []
for name in list_temp:
	t = multiprocessing.Process(target = pairtopair,args = (name,))
	t.start()
	task_list.append(t)

	if len(task_list) == 15:
		for t in task_list:
			t.join()
		time.sleep(5)
		print "\nFinish 15"
		task_list = []
for t in task_list:
	t.join()
time.sleep(5)


command = "cat temp_aa.pairToPair "
for name in list_temp:
	if name == "aa":
		continue
	command += "temp_%s.pairToPair " %(name)
command += ">temp.pairToPair"
os.system(command)

for name in list_temp:
	os.remove("./temp_split_%s" %(name))
	os.remove("./temp_%s.pairToPair" %(name))
'''

split_chr()

chrom_list = ["chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", \
	"chr10", "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", \
	"chr18", "chr19", "chr20", "chr21", "chr22", "chrX"]

list_temp = []
for chrom in chrom_list:
	list_temp.append("temp_%s.pairToPair" %(chrom))

count = 0
command = "cat tmp_withLinker_clusters_0.txt "

task_list = []

for name in list_temp:
	t = multiprocessing.Process(target = cluster,args = (name,count,))
	t.start()
	task_list.append(t)


	if len(task_list) == 10:
		for t in task_list:
			t.join()
		time.sleep(5)
		print "\nFinish 10"
		task_list = []

	if count  > 0:
		command += "tmp_withLinker_clusters_%d.txt " %(count)
	count += 1

for t in task_list:
	t.join()
time.sleep(5)


command += "> tmp_withLinker_clusters.txt"
os.system(command)

'''
for name in list_temp:
	os.remove("./chr/%s" %(name))
for name in xrange(count):
	os.remove("tmp_withLinker_clusters_%d.txt" %(name))
'''


