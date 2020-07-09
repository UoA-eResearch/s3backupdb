#!/usr/bin/python3
import os
import sys
import boto3
import botocore
from smart_open import open
import re
import hashlib
import codecs
import argparse
import json
import glob

class S3RSyncDB:
  CHUNK_SIZE = 1073741824 #1G
  ROTATION_LVL = 7 # Keep this many 

  def __init__(self,  dest_keys, dest_endpoint, chunk_size=CHUNK_SIZE, debug = 0, update = True):
    '''
      Cache s3 credentials for later use.
      
      dest_keys:     S3 key ID and secret for the destination S3 server
      dest_endpoint: URI for the destination S3 server
      self.chunk_size:    Read/write buffer size. Affects the S3 ETag, when more data size > chunk size.
    '''

    self.dest_session = boto3.Session(
         aws_access_key_id=dest_keys['access_key_id'],
         aws_secret_access_key=dest_keys['secret_access_key']
    )
    self.dest_connection = self.dest_session.client(
        's3',
        aws_session_token=None,
        region_name='us-east-1',
        use_ssl=True,
        endpoint_url=dest_endpoint,
        config=None
    )
    self.dest_endpoint = dest_endpoint
    
    self.chunk_size = chunk_size
    self.debug = debug
    self.update = update

  def read_in_chunks(self, file_object):
    '''
      Iterator to read a file chunk by chunk.
    
      file_object: file opened by caller
    '''
    while True:
      data = file_object.read(self.chunk_size)
      if not data:
        break
      yield data

  def etag(self, md5_array):
    ''' 
      Calculate objects ETag from array of chunk's MD5 sums
    
      md5_array: md5 hash of each buffer read
    '''
    if len(md5_array) < 1:
      return '"{}"'.format(hashlib.md5().hexdigest())

    if len(md5_array) == 1:
      return '"{}"'.format(md5_array[0].hexdigest())

    digests = b''.join(m.digest() for m in md5_array)
    digests_md5 = hashlib.md5(digests)
    return '"{}-{}"'.format(digests_md5.hexdigest(), len(md5_array))

  def s3copyfile(self, source_file, dest_bucket, key, size, dest_prefix = None, disable_multipart = False):
    ''' 
      S3 copy from source S3 object to destination s3 object ( renamed as src_bucket/original_object_name )
      
      source_file: Name of file to copy to S3
      dest_bucket: destination S3 bucket to copy the object to
      key:         Object name
      size:        length of object, in bytes. Used to determine if we write in chunks, or not (which affects the ETag created)
      prefix:      Change key to 'prefix/key'
      disable_multipart Don't compare the file size with the chunk size to determine if this should be a multipart upload.
    '''
    
    dest_key = '{}/{}'.format(dest_prefix, key) if dest_prefix is not None else key
    dest_s3_uri = 's3://{}/{}'.format(dest_bucket, dest_key)

    multipart = size > self.chunk_size
    if disable_multipart: multipart = False #Might have an original ETag, that wasn't multipart, but bigger than than the chunk_size.

    #Read from temporary file; Write to destination S3.
    md5s = []
    with open(source_file, 'rb') as fin:
      with open(dest_s3_uri, 'wb', transport_params={'session': self.dest_session,  'buffer_size': self.chunk_size, 'resource_kwargs': { 'endpoint_url': self.dest_endpoint}, 'multipart_upload': multipart}, ignore_ext=True) as s3_destination:
        for chunk in self.read_in_chunks(fin):
          md5s.append(hashlib.md5(chunk)) #So we can validate the upload
          s3_destination.write(chunk)
           
    #Check ETag generated is the same as the object in the store.
    calculated_etag = self.etag(md5s)
    head = self.dest_connection.head_object(Bucket=dest_bucket, Key=dest_key)
    if calculated_etag != head['ETag']:
      raise Exception( "s3copyfile({}): Etags didn't match".format(source_file) )
      
  def s3remove(dest_bucket, key, dest_prefix = None):
    dest_key = '{}/{}'.format(dest_prefix, key) if prefix is not None else key
    self.dest_connection.delete_object(Bucket = dest_bucket, Key = dest_key)

  def bucket_ls(self, s3, bucket, prefix="", suffix=""):
    '''
    Generate objects in an S3 bucket. Derived from AlexWLChan 2019

    :param s3: authenticated client session.
    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with this prefix (optional).
    :param suffix: Only fetch objects whose keys end with this suffix (optional).
    '''
    paginator = s3.get_paginator("list_objects") # should be ("list_objects_v2"), but only getting first page with this

    kwargs = {'Bucket': bucket}

    # We can pass the prefix directly to the S3 API.  If the user has passed
    # a tuple or list of prefixes, we go through them one by one.
    if isinstance(prefix, str):
      prefixes = (prefix, )
    else:
      prefixes = prefix

    for key_prefix in prefixes:
      kwargs["Prefix"] = key_prefix

      for page in paginator.paginate(**kwargs):
        try:
          contents = page["Contents"]
        except KeyError:
          break

        for obj in contents:
          key = obj["Key"]
          if key.endswith(suffix):
            yield obj

  def s3ls(self, bucket, prefix ):
    for r in self.bucket_ls(s3 = self.dest_connection, bucket = bucket, prefix = '{}/'.format(prefix)):
      print(r['Key'], ' ', r['Size'], ' ', r['LastModified'], ' ', r['ETag'])

  def backup(self, src_dir, src_pattern, dest_bucket, backup_prefix, rotation_lvl = ROTATION_LVL, rm_empty = True ):
    '''
      Copy all objects in the source S3 bucket to the destination S3 bucket
      Prefixing the destination key with the source bucket name.
    
      src_dir:       directory backup files are in
      src_pattern:   Shell filename patterns ( ?, *, [] )
      dest_bucket:   bucket name
      backup_prefix: Prefix destination key with this string ( key = "backup_prefix/filename" ).
    '''
    #Preseed the destination bucket object keys dictionary with the Object headers.
    dest_keys = {}
    for r in self.bucket_ls(s3=self.dest_connection, bucket=dest_bucket, prefix='{}/'.format(backup_prefix)):
      key = re.sub(r"^{}/".format(backup_prefix), '', r['Key']) #Remove the backup prefix, so we can match directly against file names.
      #For files, we can't get the ETag without reading them, so ETag's are checked on upload to ensure a valid copy,
      #But we assume a DB backup file then stays the same.
      dest_keys[key] = r['Size'] 
      if self.debug >= 3: print('DEST: ', r['Key'], ' ', r['ETag'], ' ', r['Size']) 
      
    #Change to the src directory, so we don't have to concatentate the dir with the file names.
    os.chdir(src_dir)

    #Get a directory list, sorted by date, in reverse order.
    #Files must match the pattern, so we don't deal with non-backup files.
    listing = sorted(glob.glob(src_pattern), key=os.path.getmtime, reverse=True)

    #Iterate over a copy of the directory listing.
    #Remove any zero length backup files.
    file_size = {}
    for f in listing[:]:
      file_size[f] = os.path.getsize(f)
      if file_size[f] == 0 and rm_empty:
        if self.debug >= 1: print("backup zero length file: rm {}".format(f))
        listing.remove(f) #Remove the entry from the backup file list
        if self.update: os.remove(f)      #Remove the file from the source directory

    #Check the most recend 7 files have been rsync'd to the S3 store
    for f in listing[None:rotation_lvl]:
      if f not in dest_keys:
        if self.debug >= 1: print("backup: s3rsync({})".format(f))
        if self.update: self.s3copyfile(source_file = f, dest_bucket = dest_bucket, key = f, size = file_size[f], dest_prefix = backup_prefix)
      else: 
        dest_keys[f] = None #So we can spot the unprocessed ones later
        
    #Keep only the last 7 versions on the local disk and in the S3 store
    for f in  listing[rotation_lvl:None]:
      if self.debug >= 1: print("backup: os.remove({})".format(f))
      if self.update: os.remove(f)
      
      if f not in dest_keys:
        if self.debug >= 3: print("backup: file {} , not in s3 to remove".format(f))
      else:
        dest_keys[f] = None #So we can spot the unprocessed ones later
        if self.debug >= 1: print("s3remove({}/{}/{})".format(dest_bucket,dest_prefix,f))
        if self.update:  self.s3remove(dest_bucket = dest_bucket, key = f, dest_prefix = backup_prefix)
    
    for dk in dest_keys:
      if dest_keys[dk] is not None:
        if self.debug >= 1: print('backup: Unexpected object in s3: postgres_backup/{}'.format(dk))
        #Might want to delete these ones. Might want to check the object key matches the file pattern too.
        #if self.update: s3remove(dest_bucket = dest_bucket, key = dk, dest_prefix = backup_prefix)

def parse_args():
  parser = argparse.ArgumentParser(description='rsync from source s3 bucket to dest s3 bucket')
  parser.add_argument('-?', action='help', default=argparse.SUPPRESS, help=argparse._('show this help message and exit'))
  parser.add_argument('-d', '--debug', dest='debug_lvl', default=0,  help="0: Off, 1: Copy/Mismatch messages, 2: Exists messages, 3: Dest ls", type=int)
  parser.add_argument('-n', '--no_rsync', dest='no_rsync', action='store_true', help='Use with Debugging. Default is to perform s3 rsync')
  parser.add_argument('-c', '--conf', dest='conf_file', help='Specify JSON conf file for source and destination')
  parser.add_argument('-a', '--auth', dest='auth_file', help='Specify JSON auth file containing s3 keys')
  parser.add_argument('--ls', dest='ls', action='store_true', help='directory listing of S3 backup')
  args = parser.parse_args()
  
  if args.conf_file is None or args.auth_file is None:
    parser.print_help(sys.stderr)
    sys.exit(1)
    
  return args

def json_load(filename):
  try:
    with open( filename ) as f:
      return json.load(f)
  except Exception as e:
    print( "json_load({}): ".format(filename), e )
    sys.exit(1)

def main():
  args = parse_args()
  auth = json_load(args.auth_file)
  conf = json_load(args.conf_file)

  if 'dest_s3_keys' not in auth: sys.exit("dest_s3_keys not defined in auth file")
  if 'dest_endpoint' not in auth: sys.exit("dest_endpoint not defined in auth file")

  if 'dest_bucket' not in conf: sys.exit("dest_bucket not defined in conf file")
  chunk_size = S3RSyncDB.CHUNK_SIZE if 'chunk_size' not in conf else conf['chunk_size']
  
  if 'backup' not in conf: sys.exit("Backup params not defined in conf file")
  if 'directory' not in conf['backup']: sys.exit("Backup params not defined in conf file")
  
  file_pattern = '*' if 'file_pattern' not in conf['backup'] else conf['backup']['file_pattern']
  rotation_lvl = S3RSyncDB.ROTATION_LVL if 'rotate_lvl' not in conf['backup'] else conf['backup']['rotate_lvl']
  dest_prefix = None if 'dest_prefix' not in conf['backup'] else conf['backup']['dest_prefix']
  
  s3rsyncdb = S3RSyncDB(dest_keys=auth['dest_s3_keys'], dest_endpoint=auth['dest_endpoint'], debug=args.debug_lvl, chunk_size=chunk_size, update=(not args.no_rsync))
  
  if args.ls:
    s3rsyncdb.s3ls( bucket = conf['dest_bucket'], prefix = dest_prefix )
  else:
    s3rsyncdb.backup(src_dir=conf['backup']['directory'], src_pattern=file_pattern, dest_bucket=conf['dest_bucket'], backup_prefix=dest_prefix, rotation_lvl=rotation_lvl )

if __name__ == "__main__":
  main()
