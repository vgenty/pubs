#! /usr/bin/env python
######################################################################
#
# Name: audit.py
#
# Purpose: Audit pubs, sam, and working disk for consistency and
#          correctness.
#
# Created: 12-May-2016  Herbert Greenlee
#
# Usage:
#
# audit.py <options>
#
# Options:
#
# --project <project> - Specify pubs project name.
# --pattern <pattern> - Specify pubs project pattern, which will be 
# --run <run>         - Run number to audit.
# --minrun <run>      - Minimum run number to audit.
# --maxrun <run>      - Maximum run number to audit.
#                       matched against existing pubs projects.
# --all               - If specified, select all pubs projects (otherwise
#                       only select enabled projects).
# --quick             - If specified, skip auditing completed subruns.
# --fix               - Fix problems (see below).
# --big               - Flag files with more than 100 events.
#
# Here are the fixes that this script knows how to do.
#
# 1.  Missing input errors.
#
#     Set the subrun status back to one.  Declare any existing files
#     in sam as bad.  This only works if the status is something
#     other than one.
#
# 2.  No file declared.
#
#     Set the subrun status back to one.
#
# 3.  No files.list or filesana.list.
#
#     Set the subrun status back to one.  Declare any existing files
#     in sam as bad.  In this case, it can sometimes work to manually 
#     rerun project.py --check(ana).
#
# 4.  Missing files in dropbox.
#
######################################################################

import sys, os
import project, project_utilities
from project_modules.pubsdeadenderror import PubsDeadEndError
from project_modules.pubsinputerror import PubsInputError

os.environ['PUB_LOGGER_LEVEL'] = 'kLOGGER_ERROR'

from dstream.ds_api import ds_writer
from pub_dbi import pubdb_conn_info

# Print help.

def help():

    filename = sys.argv[0]
    file = open(filename, 'r')

    doprint=0
    
    for line in file.readlines():
        if line[2:10] == 'audit.py':
            doprint = 1
        elif line[0:6] == '######' and doprint:
            doprint = 0
        if doprint:
            if len(line) > 2:
                print line[2:],
            else:
                print


# Basically a copy of function from production.py.

def getXML(proj_info, run):

    if not proj_info._resource.has_key('XML_TEMPLATE'):
        return proj_info._resource['XMLFILE']

    xml_path = '%s/%s_run_%07d.xml' % (proj_info._resource['XML_OUTDIR'], 
                                       proj_info._project, int(run))

    xml_template = proj_info._resource['XML_TEMPLATE']
    if not '/' in xml_template:
        xml_template = '%s/dstream_prod/xml/%s' % (os.environ['PUB_TOP_DIR'], xml_template)

    if not os.path.exists(xml_path) or \
        os.path.getctime(xml_path) < os.path.getctime(xml_template):

        print 'Remake xml file %s' % xml_path

        # Make dictionary.

        xml_rep_var = {}
        for key,value in proj_info._resource.iteritems():
            if key.startswith('PUBS_XMLVAR_'):
                xml_rep_var[key]=value

        # Make xml file using template.

        fout = open(xml_path,'w')
        contents = open(xml_template,'r').read()
        contents = contents.replace('REP_RUN_NUMBER','%d' % run)
        contents = contents.replace('REP_ZEROPAD_RUN_NUMBER','%07d' % run)
        for key,value in xml_rep_var.iteritems():
            contents = contents.replace(key,value)
        fout.write(contents)
        fout.close()

    return xml_path

# Recursively declare file and any descendants as bad.

def declare_bad(samweb, filename):
    print 'Declare bad: %s' % filename

    # Find descendants of filename and declare them bad as well.

    children = samweb.listFiles(
        dimensions='ischildof: (file_name %s and availability: anylocation) and availability: anylocation' % filename)

    for child in children:
        declare_bad(samweb, child)

    # Now declare the original file as bad.

    samweb.modifyFileMetadata(filename, md={"content_status": "bad"})

    # Done

    return


# Parse arguments.

project_arg = ''
pattern = ''
minrun_arg = 0
maxrun_arg = 0
all = 0
quick = 0
fix = 0
big = 0

args = sys.argv[1:]
while len(args) > 0:
    if args[0] == '-h' or args[0] == '--help':
        help()
        sys.exit(0)
    elif args[0] == '--all':
        all = 1
        del args[0]
    elif args[0] == '--quick':
        quick = 1
        del args[0]
    elif args[0] == '--fix':
        fix = 1
        del args[0]
    elif args[0] == '--big':
        big = 1
        del args[0]
    elif args[0] == '--project' and len(args) > 1:
        project_arg = args[1]
        del args[0:2]
    elif args[0] == '--pattern' and len(args) > 1:
        pattern = args[1]
        del args[0:2]
    elif args[0] == '--run' and len(args) > 1:
        minrun_arg = int(args[1])
        maxrun_arg = minrun_arg
        del args[0:2]
    elif args[0] == '--minrun' and len(args) > 1:
        minrun_arg = int(args[1])
        del args[0:2]
    elif args[0] == '--maxrun' and len(args) > 1:
        maxrun_arg = int(args[1])
        del args[0:2]
    else:
        print 'Unknown option %s' % args[0]
        sys.exit(1)

# DB connection.

dbi = ds_writer(pubdb_conn_info.writer_info())
try:
    dbi.connect()
    print 'Connection successful.'
except:
    print 'Connection failed.'
    sys.exit(1)

# Get samweb.

samweb = project_utilities.samweb()

# Get list of projects.

if project_arg != '' or all:
    proj_infos = dbi.list_all_projects()
else:
    proj_infos = dbi.list_projects()

# Loop over projects.

for proj_info in proj_infos:
    project_name = proj_info._project

    # Select projects.

    selected = False
    if project_arg == '' and pattern == '':
        selected = True
    if project_arg != '' and project_name == project_arg:
        selected = True
    if pattern != '' and project_name.find(pattern) >= 0:
        selected = True
    if not proj_info._resource.has_key('STAGE_STATUS'):
        selected = False
    if not proj_info._resource.has_key('STAGE_NAME'):
        selected = False

    if not selected:
        continue

    # Analyze this project.

    print
    print project_name
    print 'Enable = %d' % proj_info._enable

    # Extract some parameters from this project.

    min_status = 0
    if proj_info._resource.has_key('MIN_STATUS'):
        min_status = int(proj_info._resource['MIN_STATUS'])
    print 'Minimum status = %d' % min_status

    max_status = 9
    if proj_info._resource.has_key('MAX_STATUS'):
        max_status = int(proj_info._resource['MAX_STATUS'])
    print 'Maximum status = %d' % max_status

    parent = ''
    if proj_info._resource.has_key('PARENT'):
        parent = proj_info._resource['PARENT']
    print 'Parent project = %s' % parent

    parent_status = 0
    if proj_info._resource.has_key('PARENT_STATUS'):
        parent_status = int(proj_info._resource['PARENT_STATUS'])
    print 'Parent status = %d' % parent_status
        
    minrun = -1
    if proj_info._resource.has_key('MIN_RUN'):
        minrun = int(proj_info._resource['MIN_RUN'])
    print 'Minimum run = %d' % minrun

    maxrun = -1
    if proj_info._resource.has_key('MAX_RUN'):
        maxrun = int(proj_info._resource['MAX_RUN'])
    print 'Maximum run = %d' % maxrun

    minsubrun = -1
    if proj_info._resource.has_key('MIN_SUBRUN'):
        minsubrun = int(proj_info._resource['MIN_SUBRUN'])
    print 'Minimum subrun = %d' % minsubrun

    maxsubrun = -1
    if proj_info._resource.has_key('MAX_SUBRUN'):
        maxsubrun = int(proj_info._resource['MAX_SUBRUN'])
    print 'Maximum subrun = %d' % maxsubrun

    min_runid = (minrun, minsubrun)
    max_runid = (maxrun, maxsubrun)

    statuses = proj_info._resource['STAGE_STATUS'].split(':')
    final_status = int(statuses[-1]) + 10
    print 'Final status = %d' % final_status

    # Loop over stages.

    for i in range(len(statuses)):
        base_status = int(statuses[i])
        print 'Base status = %d' % base_status

        stage_name = proj_info._resource['STAGE_NAME'].split(':')[i]
        print 'Stage name = %s' % stage_name

        first_status = base_status + min_status
        last_status = base_status + max_status
        if i == len(statuses) - 1 and max_status == 9:
            last_status += 1
        if quick and last_status == final_status:
            last_status -= 1
        print 'First status = %d' % first_status
        print 'Last status = %d' % last_status

        check = 1
        if proj_info._resource.has_key('CHECK'):
            check = int(proj_info._resource['CHECK'].split(':')[i])
        print 'Check = %d' % check

        checkana = 0
        if proj_info._resource.has_key('CHECKANA'):
            checkana = int(proj_info._resource['CHECKANA'].split(':')[i])
        print 'CheckAna = %d' % checkana

        store = 0
        if i == len(statuses) - 1:
            store = 1
        if proj_info._resource.has_key('STORE'):
            store = int(proj_info._resource['STORE'].split(':')[i])
        print 'Store = %d' % store

        storeana = 0
        if proj_info._resource.has_key('STOREANA'):
            storeana = int(proj_info._resource['STOREANA'].split(':')[-1])
        print 'StoreAna = %d' % storeana

        if len(proj_info._command.split()) >= 4:
            table = proj_info._command.split()[3]
        else:
            table = project_name
        print 'Table = %s' % table

        # Loop over statuses.

        for status in range(last_status, first_status-1, -1):

            print
            print 'Checking status %d' % status

            # Get list of subruns for this table/status.

            if status == 1 and parent != '':
                runids = dbi.get_xtable_runs([table,parent], [status,parent_status])
            else:
                runids = dbi.get_runs(table, status)

            # Reformat flat list of (run,subrun) into a dictionary keyed by run number.

            run_subruns = {}
            for x in runids:
                run = int(x[0])
                subrun = int(x[1])
                runid = (run, subrun)
                if runid < min_runid or runid > max_runid:
                    continue
                if not run_subruns.has_key(run):
                    run_subruns[run] = []
                run_subruns[run].append(subrun)

            # Loop over runs.

            for run in sorted(run_subruns.keys(), reverse=True):

                # Run selection.

                if minrun_arg != 0 and run < minrun_arg:
                    continue
                if maxrun_arg != 0 and run > maxrun_arg:
                    continue

                # Get xml file.

                xml = getXML(proj_info, run)
                probj = project.get_project(xml, '', stage_name)
                stobj = probj.get_stage(stage_name)

                # Get sam query dimension (includes run but not subrun constraint).

                dim = project_utilities.dimensions(probj, stobj, ana=False)
                ana_dim = project_utilities.dimensions(probj, stobj, ana=True)

                # Loop over subruns.

                for subrun in run_subruns[run]:

                    print 'Run %d, subrun %d' % (run, subrun)

                    # Check sam declarations.

                    declared_files = []
                    if check and status - base_status > 7:
                        subrun_dim = '%s and run_number %d.%d' % (dim, run, subrun)
                        declared_files = samweb.listFiles(dimensions=subrun_dim)
                        if len(declared_files) == 0:
                            print '***Error no file declared for run %d, subrun %d' % (run, subrun)
                            print 'Xml = %s' % xml
                            print subrun_dim

                            # If fix requested, set status back to 1.

                            if fix:
                                update_query = 'update %s set status=1 where run=%d and subrun=%d and status=%d' % (table, run, subrun, status)
                                ok = dbi.commit(update_query)
                                if ok:
                                    print 'Status reset to 1.'
                            continue
                        else:
                            print 'Declaration OK.'
                            if len(declared_files) > 1:
                                print 'Number of declared files: %d' % len(declared_files)

                            # Check event count.
                            # This check only makes sense for swizzle merge projects.

                            if big and parent == 'prod_swizzle_filter_v3':
                                for filename in declared_files:
                                    md = samweb.getMetadata(filename)
                                    nev = 0
                                    if md.has_key('event_count') and project_name.find('notpc') < 0:
                                        nev = md['event_count']
                                    if nev > 100:
                                        print '***Error too many events: %d' % nev
                                        print '   Run %d, subrun %d' % (run, subrun)

                    # Check analysis sam declarations.

                    ana_declared_files = []
                    if checkana and status - base_status > 7:
                        ana_subrun_dim = '%s and run_number %d.%d' % (ana_dim, run, subrun)
                        ana_declared_files = samweb.listFiles(dimensions=ana_subrun_dim)
                        if len(ana_declared_files) == 0:
                            print '***Error no analysis file declared for run %d, subrun %d' % (
                                run, subrun)
                            print 'Xml = %s' % xml

                            # If fix requested, set status back to 1.

                            if fix:
                                update_query = 'update %s set status=1 where run=%d and subrun=%d and status=%d' % (table, run, subrun, status)
                                ok = dbi.commit(update_query)
                                if ok:
                                    print 'Status reset to 1.'
                            continue
                        else:
                            print 'Analysis declaration OK.'

                    # Check location.

                    if store and status - base_status  == 10:
                        for file in declared_files:
                            locs = samweb.locateFile(filenameorid=file)
                            if len(locs) == 0:
                                print '***Error no location for run %d, subrun %d' % (run, subrun)
                                print file
                                print 'Xml = %s' % xml
                                continue
                            else:
                                print 'Location OK.'

                    # Check analysis location.

                    if storeana and status - base_status  == 10:
                        for file in ana_declared_files:
                            locs = samweb.locateFile(filenameorid=file)
                            if len(locs) == 0:
                                print '***Error no analysis location for run %d, subrun %d' % (
                                    run, subrun)
                                print file
                                print 'Xml = %s' % xml
                                continue
                            else:
                                print 'Analysis location OK.'

                    # Check pubs input.  Missing pubs input will stall pubs production
                    # for stages 1-9.

                    if status - base_status >= 1 and status - base_status <= 9:

                        try:
                            pubs_probj, pubs_stobj = project.get_pubs_stage(xml, '', stage_name,
                                                                            run, [subrun], 0)
                        except PubsDeadEndError:

                            # Dead end error is OK.

                            print 'Dead end.'

                        except PubsInputError:

                            # Input error will stall pubs.

                            print '***Error input error.'
                            print 'Xml = %s' % xml

                            # Fix by setting status back to 1 (if not already).

                            if fix and status>1:
                                print 'Fixing.'

                                # Declare files bad in sam.

                                for filename in declared_files:
                                    declare_bad(samweb, filename)
                                for filename in ana_declared_files:
                                    declare_bad(samweb, filename)

                                # Reset status back to 1.

                                update_query = 'update %s set status=1 where run=%d and subrun=%d and status=%d' % (table, run, subrun, status)
                                ok = dbi.commit(update_query)
                                if ok:
                                    print 'Status reset to 1.'

                            elif fix and status == 1 and parent == 'prod_swizzle_filter_v3':
                                update_query = 'update %s set status=1 where run=%d and subrun=%d' % (parent, run, subrun)
                                ok = dbi.commit(update_query)
                                if ok:
                                    print 'Reswizzle run %d, subrun %d' % (run, subrun)

                            continue

                        except:

                            # Other errors probably shouldn't happen.

                            print '***Error unknown exception in get_pubs_stage.'
                            print 'Xml = %s' % xml
                            continue

                    # For status 7-9, check for validated files on disk.

                    if check and status - base_status >= 7 and status - base_status <= 9:

                        listpath = os.path.join(pubs_stobj.logdir, 'files.list')
                        if not project_utilities.safeexist(listpath):
                            print '***Error missing files.list'
                            print 'Xml = %s' % xml

                            # Fix by setting the status back to 1.

                            if fix:
                                print 'Fixing.'

                                # Declare files bad in sam.

                                for filename in declared_files:
                                    declare_bad(samweb, filename)
                                for filename in ana_declared_files:
                                    declare_bad(samweb, filename)

                                # Reset status back to 1.

                                update_query = 'update %s set status=1 where run=%d and subrun=%d and status=%d' % (table, run, subrun, status)
                                ok = dbi.commit(update_query)
                                if ok:
                                    print 'Status reset to 1.'

                            continue

                        validated_file_paths = project_utilities.saferead(listpath)
                        disk_file_names = []
                        for line in validated_file_paths:
                            filepath = line.strip()
                            if not project_utilities.safeexist(filepath):

                                # File is not on disk, but might be alreday on tape
                                # (in case of status 9)

                                ontape = False
                                if status - base_status == 9:
                                    print '***Error validated file not on disk (might be on tape)'
                                    ontape = True

                                if not ontape:
                                    print '***Error validated file not on disk.'
                                    print filepath
                                    print project_utilities.safeexist(filepath)
                                    print 'Xml = %s' % xml

                                    if fix and status - base_status == 7:

                                        # Reset status back to 1.

                                        update_query = 'update %s set status=1 where run=%d and subrun=%d and status=%d' % (table, run, subrun, status)
                                        ok = dbi.commit(update_query)
                                        if ok:
                                            print 'Status reset to 1.'

                                    continue
                            else:
                                disk_file_names.append(os.path.basename(filepath))

                    # For status 7-9, check for validated analysis files on disk.

                    if checkana and status - base_status >= 7 and status - base_status <= 9:

                        listpath = os.path.join(pubs_stobj.logdir, 'filesana.list')
                        if not project_utilities.safeexist(listpath):
                            print '***Error missing filesana.list'
                            print 'Xml = %s' % xml

                            # Fix by setting the status back to 1.

                            if fix:
                                print 'Fixing.'

                                # Declare files bad in sam.

                                for filename in declared_files:
                                    declare_bad(samweb, filename)
                                for filename in ana_declared_files:
                                    declare_bad(samweb, filename)

                                # Reset status back to 1.

                                update_query = 'update %s set status=1 where run=%d and subrun=%d and status=%d' % (table, run, subrun, status)
                                ok = dbi.commit(update_query)
                                if ok:
                                    print 'Status reset to 1.'

                            continue

                        validated_ana_file_paths = project_utilities.saferead(listpath)
                        disk_ana_file_names = []
                        for filepath in validated_ana_file_paths:
                            if not project_utilities.safeexist(filepath):
                                print '***Error validated analysis file not on disk.'
                                print filepath
                                print 'Xml = %s' % xml
                                continue
                            else:
                                disk_ana_file_names.append(os.path.basename(filepath))

                    # For status 9, check for file in dropbox.

                    if store and status - base_status == 9:

                        for filename in declared_files:
                            dropbox_dir = project_utilities.get_dropbox(filename)
                            dropbox_path = os.path.join(dropbox_dir, filename)
                            if not project_utilities.safeexist(dropbox_path):
                                print '***Error missing dropbox file %s' % dropbox_path
                                print 'Xml = %s' % xml
                                if fix:
                                    print 'Fixing.'

                                    # Declare files bad in sam.

                                    for filename in declared_files:
                                        declare_bad(samweb, filename)
                                    for filename in ana_declared_files:
                                        declare_bad(samweb, filename)

                                    # Reset status back to 1.

                                    update_query = 'update %s set status=1 where run=%d and subrun=%d and status=%d' % (table, run, subrun, status)
                                    ok = dbi.commit(update_query)
                                    if ok:
                                        print 'Status reset to 1.'

                    # For status 9, check for analysis files in dropbox.

                    if storeana and status - base_status == 9:

                        for filename in ana_declared_files:
                            dropbox_dir = project_utilities.get_dropbox(filename)
                            dropbox_path = os.path.join(dropbox_dir, filename)
                            if not project_utilities.safeexist(dropbox_path):
                                print '***Error missing dropbox file %s' % dropbox_path
                                print 'Xml = %s' % xml
                                if fix:
                                    print 'Fixing.'

                                    # Declare files bad in sam.

                                    for filename in declared_files:
                                        declare_bad(samweb, filename)
                                    for filename in ana_declared_files:
                                        declare_bad(samweb, filename)

                                    # Reset status back to 1.

                                    update_query = 'update %s set status=1 where run=%d and subrun=%d and status=%d' % (table, run, subrun, status)
                                    ok = dbi.commit(update_query)
                                    if ok:
                                        print 'Status reset to 1.'

