# s3backupdb
Create postgres backup with pg_backup shell script, run from cron, as the postgres user.
Rsync postgres db backups to S3 using s3rsyncdb.py.  
s3backupdb.py rotates db backup files, keeping the last "rotate_lvl" (non-zero) backup files.
S3 backups of the deleted files, are also deleted.

* Requires auth.json, with the server urls, and keys
* Requires conf.json, with the destination bucket

A file is copied to the S3 store if there is no correcsponding object of the same size at the destination. The ETag of the file is calculated and compared with the uploaded object to ensure the copy was successful.


The ETag can be an AWS style multipart MD5, but it will only match if the source and destination objects where both uploaded with the same chunk size. This code assumes 1GiB (1073741824), to match the value used for uploading instrument data. Objects of size less than, or equal to the chunk size are uploaded without using chunks, so have a standard MD5 in the ETag.

## Help
```
usage: s3backupdb [-h] [-?] [-d DEBUG_LVL] [-n] [-c CONF_FILE] [-a AUTH_FILE]

rsync from source s3 bucket to dest s3 bucket

optional arguments:
  -h, --help                        show this help message and exit
  -?                                show this help message and exit
  -d DEBUG_LVL, --debug DEBUG_LVL   0: Off, 
                                    1: Copy/Mismatch messages, 
                                    2: Exists messages,
                                    3: Dest ls
  -n, --no_rsync                    Use with Debugging. Default is to perform s3 rsync
  -c CONF_FILE, --conf CONF_FILE    Specify JSON conf file for source and destination
  -a AUTH_FILE, --auth AUTH_FILE    Specify JSON auth file containing s3 keys
  --ls                              list backed up objects in the S3 store
```

## Configuration

### conf/s3_auth.json
```json
{
  "dest_endpoint": "https://c.d",
  "dest_s3_keys": {
    "access_key_id": "xxxxxxxx",
    "secret_access_key": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
  }
}
```

### conf/s3_conf.json
```json
{
  "dest_bucket": "xxxxxx",
  "chunk_size": 1073741824,
  "backup": {
    "directory": "/var/lib/postgresql/backup",
    "file_pattern": "dbs-[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9].sql.gz",
    "dest_prefix": "myTardis/pg_backup",
    "rotate_lvl": 7
  }
}
```

