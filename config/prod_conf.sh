# SQL reader account config
export PUB_PSQL_READER_HOST=ifdb01.fnal.gov
export PUB_PSQL_READER_PORT=5437
export PUB_PSQL_READER_USER=$PUB_PROD_ACCOUNT
export PUB_PSQL_READER_ROLE="uboone_admin"
export PUB_PSQL_READER_DB=microboone_dev
export PUB_PSQL_READER_PASS=""

# SQL writer account config
export PUB_PSQL_WRITER_HOST=ifdb01.fnal.gov
export PUB_PSQL_WRITER_PORT=5437
export PUB_PSQL_WRITER_USER=$PUB_PROD_ACCOUNT
export PUB_PSQL_WRITER_ROLE="uboone_admin"
export PUB_PSQL_WRITER_DB=microboone_dev
export PUB_PSQL_WRITER_PASS=""

# SMTP account for sending an email report
export PUB_SMTP_ACCT=uboonepro
export PUB_SMTP_SRVR=smtp.gmail.com:587
export PUB_SMTP_PASS=herbgreenlee

export PUB_INDIR="/Users/yuntse/Data/uboone/pubs/test_in"
export PUB_OUTDIR="/Users/yuntse/Data/uboone/pubs/test_out"
#export PUB_SMTP_ACCT=drinkingkazu.pubs@aho.com
#export PUB_SMTP_SRVR=smtp.gmail.com:tako
#export PUB_SMTP_PASS=pubs.drinkingkazuak
