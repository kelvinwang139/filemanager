'''
find duplicate files on a specific directory
TO DO: 
	- check file size first
	- sort file size
	- check files that have the same size
	- calculate MD5
	- finally check file content with regard to bytes
'''

import os, sys, stat;

# def find_dupplicate(root):
	
def list_files(root, outfilename):
	summary = open(outfilename, 'w') 
	for(thisdir, subshere, fileshere) in os.walk(root):
		summary.write('[' + thisdir + ']\n')
		for fname in fileshere:
			path = os.path.join(thisdir,fname)
			if os.path.isfile(path):
				info = os.stat(path);
				filesize = info[stat.ST_SIZE];
				summary.write(path + "\t" + str(filesize) + "\n");
		
def sort_size(inname, outname):
	# sort all lines into a new file with ascending sorting of file size 
	sizedict = {}
	for line in open(inname):
		token = line.split()
		if len(token) >= 2:
			#print(token[0] + "\t" + token[1])
			fname = token[0];
			try:
				size = int(token[1]);
				if sizedict.get(size) == None:
					sizedict[size] = [fname]
				else:
					sizedict[size].append(fname)
			except ValueError:
				pass

	sizekeys = list(sizedict.keys())
	sizekeys.sort()
	
	#Write result to output file
	out = open(outname, "w")
	for s in sizekeys:
		listfiles = sizedict[s]
		for file in listfiles:
			out.write(str(s) + "\t" + file + "\n")
								
if __name__ == "__main__":
	firstfilename = "summary.txt"
	#list_files(sys.argv[1], firstfilename)
	secondfilename = "summary_sorted.txt"
	sort_size(firstfilename, secondfilename)