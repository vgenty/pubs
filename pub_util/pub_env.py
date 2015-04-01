import os,sys

#
# Logger message level definition & default
#
(kLOGGER_DEBUG,
 kLOGGER_INFO,
 kLOGGER_WARNING,
 kLOGGER_ERROR,
 kLOGGER_EXCEPTION) = (10,20,30,40,50)

kLOGGER_LEVEL = kLOGGER_INFO
if 'PUB_LOGGER_LEVEL' in os.environ.keys():
    exec('kLOGGER_LEVEL=int(%s)' % os.environ['PUB_LOGGER_LEVEL'])

#
# Logger drain definition & default
#

(kLOGGER_COUT,kLOGGER_FILE) = xrange(2)

kLOGGER_DRAIN = kLOGGER_COUT

if 'PUB_LOGGER_DRAIN' in os.environ.keys():

    exec('kLOGGER_DRAIN=int(%s)' % os.environ['PUB_LOGGER_DRAIN'])
    
    if not kLOGGER_DRAIN in [kLOGGER_COUT,kLOGGER_FILE]:
        sys.stderr.write('PUB_LOGGER_DEST env. value is invalid (%s)!' % os.environ['PUB_LOGGER_DRAIN'])
        sys.exit(1)

#
# Logger file drain path default
#
kLOGGER_FILE_LOCATION = os.environ['PWD']
if 'PUB_LOGGER_FILE_LOCATION' in os.environ.keys():
    exec('kLOGGER_FILE_LOCATION=\'%s\'' % os.environ['PUB_LOGGER_FILE_LOCATION'])
    if not os.path.isdir(kLOGGER_FILE_LOCATION):
        sys.stderr.write('PUB_LOGGER_FILE_LOCATION env. value is non-existing directory (%s)!' % os.environ['PUB_LOGGER_FILE_LOCATION'])
        sys.exit(1)

#
# Emailer config
#
kSMTP_ACCT = ''
kSMTP_SRVR = ''
kSMTP_PASS = ''
if 'PUB_SMTP_ACCT' in os.environ.keys():
    exec('kSMTP_ACCT=\'%s\'' % os.environ['PUB_SMTP_ACCT'])
if 'PUB_SMTP_SRVR' in os.environ.keys():
    exec('kSMTP_SRVR=\'%s\'' % os.environ['PUB_SMTP_SRVR'])
if 'PUB_SMTP_PASS' in os.environ.keys():
    exec('kSMTP_PASS=\'%s\'' % os.environ['PUB_SMTP_PASS'])

