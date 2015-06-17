#!/usr/bin/env bash

# SQL reader account config
export PUB_PSQL_READER_HOST=192.168.144.189
export PUB_PSQL_READER_PORT=""
export PUB_PSQL_READER_USER=$USER
export PUB_PSQL_READER_ROLE="uboonedaq_reader"
export PUB_PSQL_READER_DB=procdb
export PUB_PSQL_READER_PASS="${USER}argon!"
export PUB_PSQL_READER_CONN_NTRY=10
export PUB_PSQL_READER_CONN_SLEEP=10

# SQL writer account config
export PUB_PSQL_WRITER_HOST=192.168.144.189
export PUB_PSQL_WRITER_PORT=""
export PUB_PSQL_WRITER_USER=$USER
export PUB_PSQL_WRITER_ROLE="uboonedaq_admin"
export PUB_PSQL_WRITER_DB=procdb
export PUB_PSQL_WRITER_PASS="${USER}argon!"
export PUB_PSQL_WRITER_CONN_NTRY=10
export PUB_PSQL_WRITER_CONN_SLEEP=10

# SQL admin account config
export PUB_PSQL_ADMIN_HOST=192.168.144.189
export PUB_PSQL_ADMIN_PORT=""
export PUB_PSQL_ADMIN_USER=$USER
export PUB_PSQL_ADMIN_ROLE="uboonedaq_admin"
export PUB_PSQL_ADMIN_DB=procdb
export PUB_PSQL_ADMIN_PASS="${USER}argon!"
export PUB_PSQL_ADMIN_CONN_NTRY=10
export PUB_PSQL_ADMIN_CONN_SLEEP=10

# SMTP account for sending an email report
export PUB_SMTP_ACCT=uboonepro
export PUB_SMTP_SRVR=smtp.gmail.com:587
export PUB_SMTP_PASS=herbgreenlee

#export PUB_SMTP_ACCT=drinkingkazu.pubs@aho.com
#export PUB_SMTP_SRVR=smtp.gmail.com:tako
#export PUB_SMTP_PASS=pubs.drinkingkazuak
