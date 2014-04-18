#!/usr/bin/python
'''
####################################################################
Copyright Mark Lutz - Programming Python
split a file into a set of parts; join.py puts them back together
this is a customizable version of the standard Unix split command-line
utility; because it is written in Python, it also works on Windows and
can be easily modified; because it exports a function, its logic can
also be imported and reused in other applications
####################################################################
'''

import sys, os
kilobytes = 1024
megabytes = kilobytes * 1000
chunksize = int(1.4 * megabytes)                           # default: roughly a floppy

def split(fromfile, todir, chunksize = chunksize):
    if not os.path.exists(todir):                           # caller handles errors
        os.mkdir(todir)                                     # makedir, read/write parts
    else:
        for fname in os.listdir(todir):                     # delete any existing files
            os.removedirs(os.path.join(todir,fname))
    partnum = 0
    input = open(fromfile, 'rb')                            # binary: no encode, endline
    while True:                                             # eof=empty string from read
        chunk = input.read(chunksize)                       # get next part <= chunksize
        if not chunk: break
        partnum += 1
        filename = os.path.join(todir, ('part%04d' % partnum))
        fileobj = open(filename, 'wb')
        fileobj.write(chunk)
        fileobj.close()
    input.close()
    assert partnum <= 9999                                  # join sort fails if 5 digits

if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == '-help':
        print('Use: split.py [file-to-split target-dir [chunksize]]')
    else:
        if len(sys.argv) < 3:
            interactive = True
            fromfile = raw_input('File to be split? ')
            todir = raw_input('Directory to store part files? ')
        else:
            interactive = False
            fromfile, todir = sys.argv[1:2]                 # args in cmdline
            if len(sys.argv) == 4: chunksize = int(sys.argv[3])
        absfrom, absto = map(os.path.abspath, [fromfile, todir])
        print('Splitting', absfrom, 'to', absto,'by',chunksize)

        try:
            parts = split(fromfile, todir, chunksize)
        except:
            print('Error during split:')
            print(sys.exc_info()[0], sys.exc_info()[1])
        else:
            print('Split finished:', parts, 'parts are in', absto )
        if interactive: raw_input('Press Enter key') # pause if clicked



