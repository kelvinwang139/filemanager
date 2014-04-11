

'''
HOW TO RUN 
- Run as program:
	python3 find_duplicate.py [folder name]

Find duplicate files on a specific directory
TO DO: 
	- check file size first		OK
	- sort file size		OK	
	- check files that have the same size 	OK
	- check MD5 of the first few bytes 
	- calculate MD5				OK
	- multi-thread 	to process multiple folder at once 
	- check file content with regard to bytes
	- 	
'''

import os, sys, stat
import hashlib
import math
import threading
import _thread as thread
import time
import queue
from functools import partial 
from collections import defaultdict


def find_dupplicate(root):
	# get summary information about all files
	summary = "summary.txt"	
	list_files(root, summary)	
	
	# sort according to size
	summarysorted = "summary_sorted.txt"
	sort_size(summary,summarysorted)
	
	# scan the sorted file, group files with respect to their sizes
	res = []
	refsize = 0
	fnlist = []
	for line in open(summarysorted):
		token = line.split() 
		size = int(token[0])
		if size > refsize :	# 
			# do md5 comparing on this list 
			md5_dup_list_coarse = detect_md5_dup_coarse(fnlist)
			for g in md5_dup_list_coarse:
				md5_dup_list = detect_md5_dup(g)
				for g2 in  md5_dup_list:
					res.append(g2)
			del fnlist[:]
			refsize = size
		
		filename = line[len(token[0]):].strip()
		fnlist.append(filename)
	return res

# Given a list of file names, export names of the files that have the same md5s - COURSE-GRAINED
def detect_md5_dup_coarse(fnlist):
	d = {}
	v = list(map(calc_md5_coarse, fnlist))
	for i in range(len(v)):
		d.setdefault(v[i],[]).append(fnlist[i])
	return filter(lambda x:len(x)>1, d.values())
	
def calc_md5_coarse(filename):
	if os.path.isfile(filename) == False:
	 	return 0
	res = ''
	try:
		with open(filename, mode = 'rb') as f:
			d = hashlib.md5()
			buf = f.read(128)
			d.update(buf)
			res = d.hexdigest()
	except PermissionError:
		pass
	return res	

# Given a list of file names, export names of the files that have the same md5s - FIND-GRAINED
def detect_md5_dup(fnlist):
	d = {}
	v = list(map(calc_md5, fnlist))
	for i in range(len(v)):
		d.setdefault(v[i],[]).append(fnlist[i])
	return filter(lambda x:len(x)>1, d.values())
	
# Calculate MD5 of each file, based on its filename	
def calc_md5(filename):
	if os.path.isfile(filename) == False:
	 	return 0
	res = ''
	try:
		with open(filename, mode = 'rb') as f:
			d = hashlib.md5()
			for buf in iter(partial(f.read, 128), b''):
				d.update(buf)
			res = d.hexdigest()
	except PermissionError:
		pass
	return res
	
	
# list all the file names in the root folder and store to file 
def list_files(root, outfilename):
	summary = open(outfilename, 'w') 
	for(thisdir, subshere, fileshere) in os.walk(root):
		summary.write('[' + thisdir + ']\n')
		for fname in fileshere:
			path = os.path.join(thisdir,fname)
			if os.path.isfile(path):
				info = os.stat(path);
				filesize = info[stat.ST_SIZE];
				summary.write(str(filesize) + "\t" + path   + "\n");
				
MAX_NUM_OF_FILES = 10000
fileinfoQueue = queue.Queue()		# shared object, infinite size
	
# list files - multithread version
def list_files_multithread(root, outfilename):
	# spawn consumer thread - write info to file 
	
	consumerthread = threading.Thread(target = consumer, args = (outfilename,))
	consumerthread.daemon = True	#else cannot exit! 
	consumerthread.start()
	
	# spawn producer threads - get info of the files 
	threads = []
	paths = []
	for(thisdir, subshere, fileshere) in os.walk(root):
		paths.extend([os.path.join(thisdir, fname) for fname in fileshere])
		if len(paths) > MAX_NUM_OF_FILES:
			# create a new thread to store information of files in this folder
			pathscopy = paths
			print(len(pathscopy))
			thread = threading.Thread(target = getinfo, args = (pathscopy,))
			threads.append(thread)
			thread.start()
			del paths[0: len(paths)]		# reset the paths 
	for thread in threads: thread.join()

# functions for consumer thread: write file info to summary files
def consumer(outfilename):
	summary = open(outfilename,'w')
	while True:
		time.sleep(0.1)
		try:
			fileinfolist = fileinfoQueue.get(block=False)
		except queue.Empty:
			pass
		else:
			#print (len(fileinfolist))
			for fileinfo in fileinfolist:
				summary.write(fileinfo)

# functions for producer thread, get info of given paths (path = dir + filename) and put into queue, waiting to be written
def getinfo(paths):
	# build up a file info list for these paths first
	#print('Producer: receive ')
	print(len(paths))
	pathscopy = paths
	print(len(pathscopy))
	tmplist = []
	for path in pathscopy: 
		if os.path.isfile(path):
			info = os.stat(path);
			filesize = info[stat.ST_SIZE]
			tmplist.append((str(filesize) + "\t" + path + "\n"))
	
	print(len(tmplist))
	# put this list to the global queue
	fileinfoQueue.put(tmplist, block = True)
	
			
# sort a summary file, based on file size
def sort_size(inname, outname):
	# sort according to file size 
	sizedict = {}
	for line in open(inname):
		token = line.split()
		if len(token) >= 2:
			try:
				size = int(token[0]);
				line = line[len(token[0]):]
				fname = line.strip()
				
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
	### WORKING VERSION 1 ###
# 	fn1 = "summary.txt"
# 	dupp_file_group = 	find_dupplicate (sys.argv[1])
# 	print("***RESULT***")
# 	for g in dupp_file_group:
# 		print("---")
# 		for fn in g:
# 			print(fn)
	### END WORKING VERSION 1 ###
	
	### VERSION2: Doing all things in threads
# 	fn1 = "out_single.txt"
# 	
# 	start1 = time.time()
# 	list_files(sys.argv[1], fn1)
# 	end1 = time.time()
# 	print ("[Singlethread] Time elapsed:")	
# 	print (end1 - start1)
# 	
	fn2 = "out_multi.txt"
	start2 = time.time()
	list_files_multithread(sys.argv[1], fn2)
	end2 = time.time()
	print ("[Multithread] Time elapsed:")
	print (end2 - start2)
