#!/bin/sh
#Dump the myTardis databases to a timestamped gzip'd backup file
/usr/bin/pg_dumpall | gzip >  /var/lib/postgresql/backup/dbs-$(date +"%Y-%m-%d").sql.gz

