
file = open("cluster2.txt","r")
result = open("cluster2_add.txt","w")


lines = file.readlines()
count = 0
for line in lines:
	line = line.strip()
	result.write(line+"\tcluster2_%d\n" %(count))
	count += 1