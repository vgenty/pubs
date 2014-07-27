import os
os.environ['PUB_LOGGER_LEVEL']='kLOGGER_DEBUG'
os.environ['PUB_LOGGER_DRAIN']='kLOGGER_COUT'
from pub_dbi import pubdb_conn, pubdb_conn_info

conn_info = pubdb_conn_info.reader_info()
k=pubdb_conn()
k.cursor(conn_info)
conn_info._db='procdb'
k.cursor(conn_info)

