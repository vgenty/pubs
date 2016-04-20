#! /usr/bin/env python

import sys, os
import project_utilities

# Hard-wired parameters.

dim = 'file_type data and file_format root and data_tier root-tuple and ub_project.stage anatree and ub_project.version prod_v05_08_00 and availability: anylocation'

# Get samweb

samweb = project_utilities.samweb()

# Loop over files.

filelist = samweb.listFiles(dimensions=dim, stream=True)
while 1:
    try:
        filename = filelist.next()
    except StopIteration:
        break

    print filename
    md = samweb.getMetadata(filename)
    runs = []
    if md.has_key('runs'):
        runs = md['runs']
        runs.sort()
    newruns = []
    for parent in md['parents']:
        parent_name = parent['file_name']
        md_parent = samweb.getMetadata(parent_name)
        for run in md_parent['runs']:
            if not run in newruns:
                newruns.append(run)
    newruns.sort()
    if runs != newruns:
        print 'Updating metadata:'
        print newruns
        mdnew = {'runs': newruns}
        samweb.modifyFileMetadata(filename, mdnew)

sys.exit(0)
