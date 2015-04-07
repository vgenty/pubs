#!/usr/bin/env python
from pub_util import pub_logger, pub_env
from pub_dbi  import pubdb_conn_info
from dstream  import ds_daemon
from dstream.ds_api import ds_master
import os,sys

logger = pub_logger.get_logger('register_daemon')
# DB interface for altering ProcessTable
conn=ds_master(pubdb_conn_info.admin_info(),logger)

# Connect to DB
conn.connect()

def parse(contents):
    new_contents=[]
    daemon_v=[]
    ctr = 0
    for line in contents.split('\n'):

        ctr+=1
        tmpline = line.strip(' ')
        tmpline = tmpline.rstrip(' ')

        if tmpline.startswith('#'): continue

        if tmpline.find('#')>=0:
            tmpline=tmpline[0:tmpline.find('#')]

        if not tmpline: continue
        if tmpline.find('DAEMON_BEGIN') >=0 and not tmpline=='DAEMON_BEGIN':
            logger.error('Incorrect format @ line %d' % ctr)
            logger.error('%d' % line)
            logger.critical('Aborting...')
            sys.exit(1)
        new_contents.append(tmpline)

    in_block = False
    valid_keywords=('SERVER','MAX_CTR','LIFETIME','LOG_LIFETIME','SYNC_TIME',
                    'UPDATE_TIME','CLEANUP_TIME','CONTACT','ENABLE')   
    for line in new_contents:

        if line=='DAEMON_BEGIN':
            if in_block:
                logger.error('DAEMON_BEGIN found before DAEMON_END!')
                logger.critical('Aborting...')
                sys.exit(1)
            in_block = True
            daemon_v.append( ds_daemon( server='',
                                        max_proj_ctr=0,
                                        lifetime = 0,
                                        log_lifetime=0,
                                        runsync_time=0,
                                        update_time=0,
                                        cleanup_time=0,
                                        email='',
                                        enable=False) )
            continue
        elif line=='DAEMON_END':
            if not daemon_v[-1].is_valid():
                logger.error('Found a block with an incomplete daemon information...')
                logger.error('%s' % daemon_v[-1])
                logger.critical('Aborting...')
                sys.exit(1)
            in_block=False
            continue
        keyword = line.split(None)[0]
        value   = line[line.find(keyword)+len(keyword):len(line)].strip(' ')
        if not keyword in valid_keywords or len(line.split(None)) < 2:
            logger.error('Invalid syntax found in the following line!')
            logger.error(line)
            logger.critical('Aborting...')
            sys.exit(1)

        if keyword == 'SERVER':
            if daemon_v[-1]._server:
                logger.error('SERVER tag appeared twice...')
                logger.error('%s => %s' % (daemon_v[-1]._server,value))
                logger.critical('Aborting...')
                sys.exit(1)
            elif value.find(' ')>=0:
                logger.error('SERVER value contains a space!')
                logger.error('\"%s\"' % value)
                logger.critical('Aborting...')
                sys.exit(1)
            if value.upper() == 'CURRENT_SERVER':
                value = pub_env.kSERVER_NAME
            daemon_v[-1]._server = value

        elif keyword == 'CONTACT':
            value = value.replace(',',' ')
            while value.find('  ') >= 0:
                value = value.replace('  ',' ')
            daemon_v[-1]._email += '%s ' % value
            
        elif keyword == 'MAX_CTR':
            if daemon_v[-1]._max_proj_ctr:
                logger.error('MAX_CTR tag appeared twice...')
                logger.critical('Aborting...')
                sys.exit(1)
            try:
                exec('daemon_v[-1]._max_proj_ctr = int(%s)' % value)
                if daemon_v[-1]._max_proj_ctr <= 0:
                    raise ValueError
            except ValueError:
                logger.error('MAX_CTR tab value must be a positive integer!')
                logger.error('Your provided: \"%s\"' % value)
                logger.critical('Aborting...')
                sys.exit(1)

        elif keyword == 'LIFETIME':
            if daemon_v[-1]._lifetime:
                logger.error('LIFETIME tag appeared twice...')
                logger.critical('Aborting...')
                sys.exit(1)
            try:
                exec('daemon_v[-1]._lifetime = int(%s)' % value)
                if daemon_v[-1]._lifetime < 0:
                    raise ValueError
            except ValueError:
                logger.error('LIFETIME tab value must be an integer >= 0!')
                logger.error('Your provided: \"%s\"' % value)
                logger.critical('Aborting...')
                sys.exit(1)

        elif keyword == 'SYNC_TIME':
            if daemon_v[-1]._runsync_time:
                logger.error('SYNC_TIME tag appeared twice...')
                logger.critical('Aborting...')
                sys.exit(1)
            try:
                exec('daemon_v[-1]._runsync_time = int(%s)' % value)
                if daemon_v[-1]._runsync_time <= 0:
                    raise ValueError
            except ValueError:
                logger.error('SYNC_TIME tab value must be a positive integer!')
                logger.error('Your provided: \"%s\"' % value)
                logger.critical('Aborting...')
                sys.exit(1)

        elif keyword == 'UPDATE_TIME':
            if daemon_v[-1]._update_time:
                logger.error('UPDATE_TIME tag appeared twice...')
                logger.critical('Aborting...')
                sys.exit(1)
            try:
                exec('daemon_v[-1]._update_time = int(%s)' % value)
                if daemon_v[-1]._update_time <= 0:
                    raise ValueError
            except ValueError:
                logger.error('UPDATE_TIME tab value must be a positive integer!')
                logger.error('Your provided: \"%s\"' % value)
                logger.critical('Aborting...')
                sys.exit(1)

        elif keyword == 'LOG_LIFETIME':
            if daemon_v[-1]._log_lifetime:
                logger.error('LOG_LIFETIME tag appeared twice...')
                logger.critical('Aborting...')
                sys.exit(1)
            try:
                exec('daemon_v[-1]._log_lifetime = int(%s)' % value)
                if daemon_v[-1]._log_lifetime <= 0:
                    raise ValueError
            except ValueError:
                logger.error('LOG_LIFETIME tab value must be a positive integer!')
                logger.error('Your provided: \"%s\"' % value)
                logger.critical('Aborting...')
                sys.exit(1)

        elif keyword == 'CLEANUP_TIME':
            if daemon_v[-1]._cleanup_time:
                logger.error('CLEANUP_TIME tag appeared twice...')
                logger.critical('Aborting...')
                sys.exit(1)
            try:
                exec('daemon_v[-1]._cleanup_time = int(%s)' % value)
                if daemon_v[-1]._cleanup_time <= 0:
                    raise ValueError
            except ValueError:
                logger.error('CLEANUP_TIME tab value must be a positive integer!')
                logger.error('Your provided: \"%s\"' % value)
                logger.critical('Aborting...')
                sys.exit(1)

        elif keyword == 'ENABLE':
            try:
                daemon_v[-1]._enable = bool(value)
            except ValueError:
                logger.error('ENABLE tab value must be a boolean type!')
                logger.error('Your provided: \"%s\"' % value)
                logger.critical('Aborting...')
                sys.exit(1)

    if in_block:
        logger.error('Last block did not have DAEMON_END!')
        logger.critical('Aborting...')
        sys.exit(1)

    result = []
    for v in daemon_v:

        if conn.daemon_exist(v._server):

            logger.info('Daemon %s already exist in DB!' % v._server)
            orig_info = conn.daemon_info(v._server)

            diff = orig_info.diff(v)
            if not diff:
                logger.info('Configuration for daemon %s is identical...' % v._server)
                continue
            else:
                logger.info('Diff information below...\n%s' % diff)

        else:
            logger.info('New daemon @ %s' % v._server)

        result.append(v)

    return result

def register(daemons):

    if not daemons:
        logger.info('No update needed.')
        return True
    
    logger.info('Below is a summary of daemon update/registration.')
    # Make sure these daemons can be registered
    for d in daemons:
        logger.warning('Will register/update daemon %s...' % d._server)

    if not conn._ask_binary():
        return False

    status = True
    
    for d in daemons:

        conn.define_daemon(d)

    return status

if __name__ == '__main__':

    if len(sys.argv)<2:
        print 'Usage:',sys.argv[0],'$CONFIG_FILE'
        sys.exit(1)

    c = open(sys.argv[1],'r').read()

    daemons = parse(c)

    register(daemons)
