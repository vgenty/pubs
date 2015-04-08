#!/usr/bin/env python
from pub_util import pub_logger
from pub_dbi  import pubdb_conn_info
from dstream  import ds_project
from dstream.ds_api import ds_master
import os,sys

logger = pub_logger.get_logger('register_project')
# DB interface for altering ProcessTable
conn=ds_master(pubdb_conn_info.admin_info(),logger)

# Connect to DB
conn.connect()

def parse(contents):
    new_contents=[]
    project_v=[]
    ctr = 0
    for line in contents.split('\n'):

        ctr+=1
        tmpline = line.strip(' ')
        tmpline = tmpline.rstrip(' ')

        if tmpline.startswith('#'): continue

        if tmpline.find('#')>=0:
            tmpline=tmpline[0:tmpline.find('#')]

        if not tmpline: continue
        if tmpline.find('PROJECT_BEGIN') >=0 and not tmpline=='PROJECT_BEGIN':
            logger.error('Incorrect format @ line %d' % ctr)
            logger.error('%d' % line)
            logger.critical('Aborting...')
            sys.exit(1)

        new_contents.append(tmpline)

    in_block = False
    valid_keywords=('NAME','COMMAND','CONTACT','PERIOD','SERVER','SLEEP',
                    'RUNTABLE','RUN','SUBRUN','ENABLE','RESOURCE')
    for line in new_contents:

        if line=='PROJECT_BEGIN':
            if in_block:
                logger.error('PROJECT_BEGIN found before PROJECT_END!')
                logger.critical('Aborting...')
                sys.exit(1)
            in_block = True
            project_v.append( ds_project('') )
            continue
        elif line=='PROJECT_END':
            if not project_v[-1].is_valid():
                logger.error('Found a block with an incomplete project information...')
                logger.error('%s' % project_v[-1])
                logger.critical('Aborting...')
                sys.exit(1)

            in_block=False
            continue

        keyword = line.split(None)[0]
        value   = line.replace(keyword,'').strip(' ')
        if ( not keyword in valid_keywords or
             (keyword not in ['RUNTABLE','SLEEP'] and len(line.split(None)) < 2) ):
            logger.error('Invalid syntax found in the following line!')
            logger.error(line)
            logger.critical('Aborting...')
            sys.exit(1)
            
        if keyword == 'NAME':
            if project_v[-1]._project:
                logger.error('NAME tag appeared twice...')
                logger.error('%s => %s' % (project_v[-1]._project,value))
                logger.critical('Aborting...')
                sys.exit(1)
            elif value.find(' ')>=0:
                logger.error('NAME value contains a space!')
                logger.error('\"%s\"' % value)
                logger.critical('Aborting...')
                sys.exit(1)
            project_v[-1]._project = value

        elif keyword == 'COMMAND':
            if project_v[-1]._command:
                logger.error('COMMAND tag appeared twice...')
                logger.critical('Aborting...')
                sys.exit(1)
            project_v[-1]._command = value

        elif keyword == 'CONTACT':
            value = value.replace(',',' ')
            while value.find('  ') >= 0:
                value = value.replace('  ',' ')
            project_v[-1]._email += '%s ' % value

        elif keyword == 'PERIOD':
            if project_v[-1]._period:
                logger.error('PERIOD tag appeared twice...')
                logger.critical('Aborting...')
                sys.exit(1)
            try:
                project_v[-1]._period = int(value)
                if project_v[-1]._period <= 0:
                    raise ValueError
            except ValueError:
                logger.error('PERIOD tab value must be a positive integer!')
                logger.error('Your provided: \"%s\"' % value)
                logger.critical('Aborting...')
                sys.exit(1)

        elif keyword == 'SERVER':
            if project_v[-1]._server:
                logger.error('SERVER tag appeared twice...')
                logger.critical('Aborting...')
                sys.exit(1)
            elif value.find(' ')>=0:
                logger.error('SERVER value contains a space!')
                logger.error('\"%s\"' % value)
                logger.critical('Aborting...')
                sys.exit(1)
            if value.upper() == 'CURRENT_SERVER':
                value = pub_env.kSERVER_NAME
            project_v[-1]._server = str(value)

        elif keyword == 'RUNTABLE':
            if project_v[-1]._runtable:
                logger.error('RUNTABLE tag appeared twice...')
                logger.critical('Aborting...')
                sys.exit(1)
            project_v[-1]._runtable = str(value)

        elif keyword == 'SLEEP':
            if project_v[-1]._sleep:
                logger.error('SLEEP tag appeared twice...')
                logger.critical('Aborting')
                sys.exit(1)
            project_v[-1]._sleep = int(value)

        elif keyword == 'RUN':
            if project_v[-1]._run:
                logger.error('RUN tag appeared twice...')
                logger.critical('Aborting...')
                sys.exit(1)
            try:
                project_v[-1]._run = int(value)
                if project_v[-1]._run < 0:
                    raise ValueError
            except ValueError:
                logger.error('RUN tab value must be an integer >= 0!')
                logger.error('Your provided: \"%s\"' % value)
                logger.critical('Aborting...')
                sys.exit(1)
            
        elif keyword == 'SUBRUN':
            if project_v[-1]._subrun:
                logger.error('SUBRUN tag appeared twice...')
                logger.critical('Aborting...')
                sys.exit(1)
            try:
                project_v[-1]._subrun = int(value)
                if project_v[-1]._subrun < 0:
                    raise ValueError
            except ValueError:
                logger.error('SUBRUN tab value must be an integer >= 0!')
                logger.error('Your provided: \"%s\"' % value)
                logger.critical('Aborting...')
                sys.exit(1)

        elif keyword == 'ENABLE':
            try:
                if not value.lower() in ['true','false','0','1']:
                    raise ValueError
                project_v[-1]._enable = value.lower() in ['true','1']
            except ValueError:
                logger.error('ENABLE tab value must be a boolean type!')
                logger.error('Your provided: \"%s\"' % value)
                logger.critical('Aborting...')
                sys.exit(1)

        elif keyword == 'RESOURCE':
            
            key_and_value = value.split("=>")
            if not len(key_and_value) == 2:
                logger.error('RESOURCE tag must by in a format key => value!')
                logger.error('\"%s\"' % line)
                logger.critical('Aborting...')
                sys.exit(1)
            
            if key_and_value[0] in project_v[-1]._resource.keys():
                logger.error('RESOURCE tag has a duplicate key: %s!' % key_and_value[0])
                logger.critical('Aborting...')
                sys.exit(1)
            
            key_and_value[0] = key_and_value[0].strip(' ')
            key_and_value[0] = key_and_value[0].rstrip(' ')
            key_and_value[1] = key_and_value[1].strip(' ')
            key_and_value[1] = key_and_value[1].rstrip(' ')
            project_v[-1]._resource[key_and_value[0]] = key_and_value[1]

    if in_block:
        logger.error('Last block did not have PROJECT_END!')
        logger.critical('Aborting...')
        sys.exit(1)

    result = []
    for v in project_v:

        if conn.project_exist(v._project):

            logger.info('Project %s already exist in DB!' % v._project)
            orig_info = conn.project_info(v._project)

            if not orig_info._run == project_v[-1]._run:
                logger.error('Your configuration has different run number (not allowed)!')
                logger.critical('Aborting...')
                sys.exit(1)
            if not orig_info._subrun == project_v[-1]._subrun:
                logger.error('Your configuration has different sub-run number (not allowed)!')
                logger.critical('Aborting...')
                sys.exit(1)
            if not orig_info._runtable == project_v[-1]._runtable:
                logger.error('Your configuration has different run-table name (not allowed)!')
                logger.critical('Aborting...')
                sys.exit(1)

            diff = orig_info.diff(v)
            if not diff:
                logger.info('Configuration for project %s is identical...' % v._project)
                continue
            else:
                logger.warning('Diff information below...\n%s' % diff)

        logger.info('Valid project info...\n%s' % v)
        result.append(v)

    return result

def register(projects):

    if not projects:
        logger.info('No update needed.')
        return True
    
    logger.warning('Below is a summary of project update/registration.')
    # Make sure these projects can be registered
    for p in projects:
        logger.warning('Will register/update project %s...' % p._project)

    if not conn._ask_binary():
        return False

    status = True
    
    for p in projects:

        if conn.project_exist(p._project):
            
            status = status and conn.update_project(p,False)

        else:

            status = status and conn.define_project(p)

    return status

if __name__ == '__main__':

    if len(sys.argv)<2:
        print 'Usage:',sys.argv[0],'$CONFIG_FILE'
        sys.exit(1)

    c = open(sys.argv[1],'r').read()

    projects = parse(c)

    register(projects)
