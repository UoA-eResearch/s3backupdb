#!/bin/sh
chdir /var/lib/postgresql
#Dump the myTardis databases to a timestamped gzip'd backup file
/usr/bin/pg_dumpall | gzip >  backup/dbs-$(date +"%Y-%m-%d").sql.gz

#copy backup files to S3. Side effect is to remove failed 0 size backups
bin/s3backupdb -d 1 -c etc/s3_backup_conf.json -a etc/s3_auth.json
