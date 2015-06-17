#!/usr/bin/env python
from pub_util import pub_logger, pub_env
from pub_dbi  import pubdb_conn_info
from dstream  import ds_project
from dstream.ds_api import ds_master
import register_project
import os,sys
import time

def register(conn,logger,projects):


    if not projects:
        logger.info('No update needed.')
        return True
    
    logger.warning('Below is a summary of project to bump-up version number.')
    for p in projects:
        logger.info('Bump-up project version for %s ...' % p._project)        

    if not conn._ask_binary():
        return False

    status = True
    for p in projects:

        if conn.project_exist(p._project):

            status = status and conn.project_version_update(p)

        else:

            status = status and conn.define_project(p)

    return status


if __name__ == '__main__':

    logger = pub_logger.get_logger('register_project')
    # DB interface for altering ProcessTable
    conn=ds_master(pubdb_conn_info.admin_info(),logger)
    
    # Connect to DB
    conn.connect()
    
    if len(sys.argv)<2:
        print 'Usage:',sys.argv[0],'$CONFIG_FILE'
        sys.exit(1)

    c = open(sys.argv[1],'r').read()

    projects = register_project.parse(conn,logger,c)

    register(conn,logger,projects)


