'''
Copyright Mark Lutz Programming Python
Find the largest Python source file in an entire directory tree.
Search the Python source lib, use ppprint to display results nicely
'''

import os, sys, pprint
trace = False
	
dirname = sys.argv[1]
allsizes = []
for (thisDir,subsHere, filesHere) in os.walk(dirname):
	if trace: print(thisDir)
	for filename in filesHere:
		if filename.endswith('.py'):
			if trace: print('...', filename)
			fullname = os.path.join(thisDir, filename)
			fullsize = os.path.getsize(fullname)
			allsizes.append((fullsize, fullname))

allsizes.sort()
pprint.pprint(allsizes[:2])
pprint.pprint(allsizes[-2:])