

'''

Find duplicate files in an arbitrary directory tree.

HOW TO RUN
	python3 find_duplicate.py [directory name]

TO DO: 
	- check file size first		OK
	- sort file size		OK	
	- check files that have the same size 	OK
	- check MD5 of the first few bytes 
	- calculate MD5				OK
	- multi-thread 	to process multiple folder at once  OK 
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
import copy 
import multiprocessing
from multiprocessing import Process, Queue, Pool
from functools import partial 
from collections import defaultdict


def find_dupplicate(root, multiprocess = False):
	# get summary information about all files
	summary = "summary.txt"	
	if(multiprocess == True):
		print("multi")
		list_files_multiprocess(root, summary)
	else:
		print("single")
		list_files(root, summary)	
	
	# sort according to size
	summarysorted = "summary_sorted.txt"
	sort_size(summary,summarysorted)
	
	res = []
	refsize = 0
	fnlist = []
	
	# for multiprocessing
	post = Queue()
	pros = []	# list of processes we init
	pool = multiprocessing.Pool(10)
	
	proc_count = 0
	
	# scan the sorted file, group files with respect to their sizes
	for line in open(summarysorted):
		token = line.split() 
		size = int(token[0])
		if size > refsize :
			if multiprocess == False:
                # do md5 comparing on this list
				md5_dup_list_coarse = detect_md5_dup_coarse(fnlist)
				for g in md5_dup_list_coarse :
					md5_dup_list = detect_md5_dup(g)
					for g2 in  md5_dup_list:
						res.append(g2)
				del fnlist[:]
				refsize = size
			else:
			    # create a new process to store information of files in this folder
				fnlistcopy = copy.deepcopy(fnlist)
				
				p = MD5Checker(fnlistcopy, post)
				p.start()
				pros.append(p)
				del fnlist[:]
				refsize = size

		filename = line[len(token[0]):].strip()
		fnlist.append(filename)
		
	# the remaining files 
	if multiprocess == False:
		# do md5 comparing on this list
		md5_dup_list_coarse = detect_md5_dup_coarse(fnlist)
		for g in md5_dup_list_coarse :
			md5_dup_list = detect_md5_dup(g)
			for g2 in  md5_dup_list:
				res.append(g2)
		del fnlist[:]
	else:
		# create a new process to store information of files in this folder
		fnlistcopy = copy.deepcopy(fnlist)
		
		p = MD5Checker(fnlistcopy, post)
		p.start()
		pros.append(p)
		del fnlist[:]
	
	if multiprocess == True:
		# wait for all processes done
		for p in pros:
			p.join()
			
		# get results from Queue
		while(post.empty() == False):
			try:
				dupfnlist = post.get(block=True)
			except queue.empty:
				pass
			else:
				res.append(dupfnlist)
	
	return res

#############################
##### MULTIPROCESSING
#############################
# producer process
class MD5Checker(Process):
	def __init__(self, paths, queue):
		self.fnlist = paths
		self.post = queue
		Process.__init__(self)

	def run(self):
		md5_dup_list_coarse = detect_md5_dup_coarse(self.fnlist)
		for g in md5_dup_list_coarse:
			md5_dup_list = detect_md5_dup(g)
			for g2 in md5_dup_list:
				# put this dupp file list to the global queue
				self.post.put(g2, block = True)


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

# Given a list of file names, export names of the files that have the same md5s - FINE-GRAINED
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
		#summary.write('[' + thisdir + ']\n')
		for fname in fileshere:
			path = os.path.join(thisdir,fname)
			if os.path.isfile(path):
				info = os.stat(path);
				filesize = info[stat.ST_SIZE];
				summary.write(str(filesize) + "\t" + path   + "\n");
				
MAX_NUM_OF_FILES = 100
fileinfoQueue = queue.Queue()		# shared object, infinite size
	


#############################
##### MULTIPROCESSING 
#############################
# producer process
class Filelister(Process):
	label = ' @'
	def __init__(self, paths, queue):
		self.paths = paths
		self.post = queue
		Process.__init__(self)
		
	def run(self):
		tmplist = []
		for path in self.paths:
			if os.path.isfile(path):
				info = os.stat(path);
				filesize = info[stat.ST_SIZE]
				tmplist.append((str(filesize) + "\t" + path + "\n"))	
		# put this list to the global queue
		self.post.put(tmplist, block = True)				
			
# consumer process
class Fileinfowriter(Process):
	label = ' @w'
	def __init__(self, outfilename, queue):
		self.out = outfilename
		self.post = queue
		Process.__init__(self)
		
	def run(self):
		summary = open(self.out, 'w')
		maxretries = 10
		retries = 0
		while True:
			if(self.post.empty()):
				retries = retries + 1
				if(retries > maxretries):
					break
				time.sleep(0.1)
			else:
				retries = 0
				try:
					infolist = self.post.get(block=False)
				except queue.empty:
					print('no data...')
				else:
					for info in infolist:
						summary.write(info)
				
				
# list files - multiprocess version
def list_files_multiprocess(root, outfilename):
	# spawn producer processes - get info of the files
	ps = []
	paths = []
	post = Queue()
	c = Fileinfowriter(outfilename, post)
	c.start()
	for (thisdir, subshere, fileshere) in os.walk(root):
		paths.extend([os.path.join(thisdir, fname) for fname in fileshere])
		if len(paths) > MAX_NUM_OF_FILES:
			# create a new process to store information of files in this folder
			pathscopy = copy.deepcopy(paths)
			p = Filelister(pathscopy, post)
			ps.append(p)
			p.start()
			del paths[0: len(paths)]
		
		# the remaining files
		pcopy = copy.deepcopy(paths)
		p = Filelister(pcopy, post)
		ps.append(p)
		p.start()
		del paths[:]
		
	total = len(ps)
	for p in ps: 
		p.join()
	c.join()
	
#############################
##### MULTITHREADING
#############################	
# functions for consumer thread: write file info to summary files
def consumer(outfilename):
	summary = open(outfilename,'w')
	maxretries = 10
	retries = 0
	while True:
		if(fileinfoQueue.empty()):
			retries = retries + 1
			if (retries > maxretries):
				break
			time.sleep(0.1)
		else:
			retries = 0
			try:
				fileinfolist = fileinfoQueue.get(block=False)
			except queue.Empty:
				pass
			else:
				for fileinfo in fileinfolist:
					summary.write(fileinfo)

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
			pathscopy = copy.deepcopy(paths)
			thread = threading.Thread(target = getinfo, args = (pathscopy,))
			threads.append(thread)
			thread.start()
			del paths[0: len(paths)]		# reset the paths 
	total = len(threads)
	count = 0
	for thread in threads: 
		thread.join()		
		count = count + 1
		# print(count * 100 / total)
	consumerthread.join()
	
# functions for producer thread, get info of given paths (path = dir + filename) and put into queue, waiting to be written
def getinfo(paths):
	# build up a file info list for these paths first
	pathscopy = copy.deepcopy(paths)
	tmplist = []
	for path in pathscopy: 
		if os.path.isfile(path):
			info = os.stat(path);
			filesize = info[stat.ST_SIZE]
			tmplist.append((str(filesize) + "\t" + path + "\n"))
	
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
	dupp_file_group = 	find_dupplicate (sys.argv[1], multiprocess = True)
	print("***RESULT***")
	for g in dupp_file_group:
		print("---")
		for fn in g:
			print(fn)	
		print("---")
	
	### TEST singlethread - multithread - multiprocessing performance
# 	fn1 = "out_single.txt"
# 	
# 	start1 = time.time()
# 	list_files(sys.argv[1], fn1)
# 	end1 = time.time()
# 	print ("[Singlethread] Time elapsed:")	
# 	print (end1 - start1)
# 	
# 	fn2 = "out_multithread.txt"
# 	start2 = time.time()
# 	list_files_multithread(sys.argv[1], fn2)
# 	end2 = time.time()
# 	print ("[Multithread] Time elapsed:")
# 	print (end2 - start2)
# 	
# 	fn3 = "out_multiprocess.txt"
# 	start3 = time.time()
# 	list_files_multiprocess(sys.argv[1], fn3)
# 	end3 = time.time()
# 	print ("[Multiprocess] Time elapsed:")
# 	print (end3 - start3)
