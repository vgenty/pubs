echo "Setup pubs"
source /home/vgenty/sw/pubs/config/setup.sh
echo "Setup online"
source /uboonenew/setup_online.sh
echo "Setup gcc"
setup gcc v4_9_2
echo "Setup python"
setup python v2_7_9
echo "Setup postgres"
setup postgresql v9_3_6 -q p279
echo "Setup psycopg"
setup psycopg2 v2_5_4
echo "Setup git"
setup git v2_3_0
echo "Setup sam"
setup sam_web_client
echo "Setup ifdhc"
setup ifdhc
#echo "kx509"
#kx509

# SQL reader account config
export PUB_PSQL_READER_HOST=genty.servebeer.com
export PUB_PSQL_READER_USER=vgenty
export PUB_PSQL_READER_ROLE=""
export PUB_PSQL_READER_DB=procdb
export PUB_PSQL_READER_PASS=""
export PUB_PSQL_READER_CONN_NTRY=10
export PUB_PSQL_READER_CONN_SLEEP=10

# SQL writer account config
export PUB_PSQL_WRITER_HOST=genty.servebeer.com
export PUB_PSQL_WRITER_USER=vgenty
export PUB_PSQL_WRITER_ROLE=""
export PUB_PSQL_WRITER_DB=procdb
export PUB_PSQL_WRITER_PASS=""
export PUB_PSQL_WRITER_CONN_NTRY=10
export PUB_PSQL_WRITER_CONN_SLEEP=10

# SQL admin account config
export PUB_PSQL_ADMIN_HOST=genty.servebeer.com
export PUB_PSQL_ADMIN_USER=vgenty
export PUB_PSQL_ADMIN_ROLE=""
export PUB_PSQL_ADMIN_DB=procdb
export PUB_PSQL_ADMIN_PASS=""
export PUB_PSQL_ADMIN_CONN_NTRY=10
export PUB_PSQL_ADMIN_CONN_SLEEP=10

# SMTP account for sending an email report
export PUB_SMTP_ACCT=pupinjesus
export PUB_SMTP_SRVR=smtp.gmail.com:587
export PUB_SMTP_PASS=pupinjesus1

export PUB_INDIR=""
export PUB_OUTDIR=""