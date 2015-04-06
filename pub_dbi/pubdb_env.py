import os

#
# Reader default configuration
#
kREADER_HOST = 'localhost'
kREADER_PORT = None
kREADER_DB   = 'procdb'
kREADER_USER = os.environ['USER']
kREADER_ROLE = ''
kREADER_PASS = ''

if 'PUB_PSQL_READER_HOST' in os.environ.keys():
    kREADER_HOST = os.environ['PUB_PSQL_READER_HOST']

if 'PUB_PSQL_READER_PORT' in os.environ.keys():
    kREADER_PORT = os.environ['PUB_PSQL_READER_PORT']
    
if 'PUB_PSQL_READER_DB' in os.environ.keys():
    kREADER_DB = os.environ['PUB_PSQL_READER_DB']
    
if 'PUB_PSQL_READER_USER' in os.environ.keys():
    kREADER_USER = os.environ['PUB_PSQL_READER_USER']

if 'PUB_PSQL_READER_ROLE' in os.environ.keys():
    kREADER_ROLE = os.environ['PUB_PSQL_READER_ROLE']
    
if 'PUB_PSQL_READER_PASS' in os.environ.keys():
    kREADER_PASS = os.environ['PUB_PSQL_READER_PASS']

#
# Writer default configuration
#
kWRITER_HOST = 'localhost'
kWRITER_PORT = ''
kWRITER_DB   = 'procdb'
kWRITER_USER = os.environ['USER']
kWRITER_ROLE = ''
kWRITER_PASS = ''

if 'PUB_PSQL_WRITER_HOST' in os.environ.keys():
    kWRITER_HOST = os.environ['PUB_PSQL_WRITER_HOST']

if 'PUB_PSQL_WRITER_PORT' in os.environ.keys():
    kWRITER_PORT = os.environ['PUB_PSQL_WRITER_PORT']
    
if 'PUB_PSQL_WRITER_DB' in os.environ.keys():
    kWRITER_DB = os.environ['PUB_PSQL_WRITER_DB']
    
if 'PUB_PSQL_WRITER_USER' in os.environ.keys():
    kWRITER_USER = os.environ['PUB_PSQL_WRITER_USER']

if 'PUB_PSQL_WRITER_ROLE' in os.environ.keys():
    kWRITER_ROLE = os.environ['PUB_PSQL_WRITER_ROLE']
    
if 'PUB_PSQL_WRITER_PASS' in os.environ.keys():
    kWRITER_PASS = os.environ['PUB_PSQL_WRITER_PASS']

