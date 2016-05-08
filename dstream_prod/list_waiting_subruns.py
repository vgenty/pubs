#! /usr/bin/env python

import sys, os
os.environ['PUB_LOGGER_LEVEL'] = 'kLOGGER_ERROR'

from dstream.ds_api import ds_reader
from pub_dbi import pubdb_conn_info

# DB connection.

dbi = ds_reader(pubdb_conn_info.reader_info())
try:
    dbi.connect()
    print "Connection successful."
except:
    print "Connection failed."
    sys.exit(1)


# Loop over projects.

for probj in dbi.list_projects():
    project = probj._project
    if project == 'prod_define_swizzler_dataset':
        continue
    if project.find('mcc7') >= 0:
        continue
    if project.find('pnnl') >= 0:
        continue
    print '\nProject %s:' % project
    
    # Get parent project resource.

    parent = ''
    if probj._resource.has_key('PARENT'):
        parent = probj._resource['PARENT']

    # Get parent status resource.

    parent_status = 0
    if probj._resource.has_key('PARENT_STATUS'):
        parent_status = int(probj._resource['PARENT_STATUS'])

    # Get minimum run resource.

    minrun = -1
    if probj._resource.has_key('MIN_RUN'):
        minrun = int(probj._resource['MIN_RUN'])

    # Get maximum run resource.

    maxrun = -1
    if probj._resource.has_key('MAX_RUN'):
        maxrun = int(probj._resource['MAX_RUN'])

    # Construct query.

    query = 'select %s.run,%s.subrun from %s' % (project, project, project)
    if parent != '':
        query += ',%s' % parent
    query += ' where %s.status=1' % project

    # Optionally add minimum run clause.

    if minrun >= 0:
        query += ' and %s.run >= %d' % (project, minrun)

    # Optionally add maximum run clause.

    if maxrun >= 0:
        query += ' and %s.run <= %d' % (project, maxrun)

    # Optionally add parent clause.

    if parent != '':
        query += ' and %s.run=%s.run and %s.subrun=%s.subrun and %s.status=%d' % (
            project, parent, project, parent, parent, parent_status)

    # Execute query.

    ok = dbi.execute(query)
    if ok and dbi.nrows()>0:
        for row in dbi:
            run = int(row[0])
            subrun = int(row[1])
            print run, subrun

sys.exit(0)
