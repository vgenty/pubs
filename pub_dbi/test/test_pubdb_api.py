import os
os.environ['PUB_LOGGER_LEVEL']='kLOGGER_DEBUG'
os.environ['PUB_LOGGER_DRAIN']='kLOGGER_COUT'
from pub_dbi import pubdb_reader, pubdb_writer
from pub_dbi import pubdb_conn_info

conn_info = pubdb_conn_info.reader_info()

k=pubdb_reader(conn_info)
k=pubdb_writer(conn_info)

ctr=0
for x in k:
    ctr+=1
    print ctr

ctr=0
for x in k:
    ctr+=1
    print ctr
