import os
os.environ['PUB_LOGGER_LEVEL']='kLOGGER_DEBUG'
os.environ['PUB_LOGGER_DRAIN']='kLOGGER_COUT'
from pub_dbi import pubdb_reader, pubdb_writer, pubdb_master
from pub_dbi import pubdb_conn_info, pubdb_status_info

conn_info = pubdb_conn_info.reader_info()

k=pubdb_reader(conn_info)
k=pubdb_writer(conn_info)
k=pubdb_master(conn_info)
j=pubdb_status_info()

k.insert_newrun(j)
#help(k._cursor)
k.query('SELECT * FROM ConfigLookUp;')

ctr=0
for x in k:
    ctr+=1
    print ctr

ctr=0
for x in k:
    ctr+=1
    print ctr
