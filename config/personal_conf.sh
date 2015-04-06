# SQL reader account config
export PUB_PSQL_READER_HOST=192.168.144.189
export PUB_PSQL_READER_USER=$USER
# export PUB_PSQL_READER_USER=echurch
export PUB_PSQL_READER_DB=procdb
export PUB_PSQL_READER_PASS="${USER}argon!"

# SQL writer account config
export PUB_PSQL_WRITER_HOST=192.168.144.189
export PUB_PSQL_WRITER_USER=$USER
# export PUB_PSQL_WRITER_USER=echurch
export PUB_PSQL_WRITER_DB=procdb
export PUB_PSQL_WRITER_PASS="${USER}argon!"

# SMTP account for sending an email report
export PUB_SMTP_ACCT=drinkingkazu.pubs@gmail.com
export PUB_SMTP_SRVR=smtp.gmail.com:587
export PUB_SMTP_PASS=pubs.drinkingkazu

#export PUB_SMTP_ACCT=drinkingkazu.pubs@aho.com
#export PUB_SMTP_SRVR=smtp.gmail.com:tako
#export PUB_SMTP_PASS=pubs.drinkingkazuak
