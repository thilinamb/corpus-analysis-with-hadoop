#!/usr/bin/python

import os
import sys

def main(argv):
	print argv[1]
	for file in os.listdir(argv[1]):
    		if file.endswith(".txt"):
        		print file

if __name__ == '__main__':
	main(sys.argv)
