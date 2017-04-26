#!/usr/bin/env python
"""
A dummy worker used to illustrate a pyflow 'launch-tasks-until-condition' workflow.

This worker briefly waits, then chooses a random number between 1-10 and writes this out to
the file specified in arg1.
"""

import sys

if len(sys.argv) != 2 :
    print "usage: $0 outputFile"
    sys.exit(2)

outputFile=sys.argv[1]

import time
time.sleep(3)

from random import randint
reportVal=randint(1,10)

ofp=open(outputFile,"wb")
ofp.write("%i\n" % (reportVal))
ofp.close()

