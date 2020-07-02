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

class S3RSyncDB:
  CHUNK_SIZE = 1073741824 #1G

  def __init__(self, src_directory, dest_keys, dest_endpoint, chunk_size=CHUNK_SIZE, debug = 0, update = True):
    '''
      Cache s3 credentials for later use.
      
      src_keys:      S3 key ID and secret of the source S3 server.
      src_endpoint:  URI for the source S3 server
      dest_keys:     S3 key ID and secret for the destination S3 server
      dest_endpoint: URI for the destination S3 server
      self.chunk_size:    Read/write buffer size. Affects the S3 ETag, when more data size > chunk size.
    '''

    self.src_session = boto3.Session(
         aws_access_key_id=src_keys['access_key_id'],
         aws_secret_access_key=src_keys['secret_access_key']
    )
    self.src_connection = self.src_session.client(
        's3',
        aws_session_token=None,
        region_name='us-east-1',
        use_ssl=True,
        endpoint_url=src_endpoint,
        config=None
    )
    self.src_endpoint = src_endpoint

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

  def s3copys3(self, dest_bucket, key, size, disable_multipart = False):
    ''' 
      S3 copy from source S3 object to destination s3 object ( renamed as src_bucket/original_object_name )
      
      src_bucket:  Source S3 bucket for the object to be copied
      dest_bucket: destination S3 bucket to copy the object to
      key:         Object name
      size:        length of object, in bytes. Used to determine if we write in chunks, or not (which affects the ETag created)
    '''
    source_s3_uri = 's3://{}/{}'.format(src_bucket, key)
    dest_s3_uri = 's3://{}/{}/{}'.format(dest_bucket, src_bucket, key)

    multipart = size > self.chunk_size
    if disable_multipart: multipart = False #Might have an original ETag, that wasn't multipart, but bigger than than the chunk_size.

    '''
    #Bug in S3 library corrupts read buffer when more than one s3 stream is open. 

    with open(source_s3_uri, 'rb', transport_params={'session': self.src_session,  'buffer_size': self.chunk_size, 'resource_kwargs': { 'endpoint_url': self.src_endpoint}}, ignore_ext=True) as s3_source:
      with open(dest_s3_uri, 'wb', transport_params={'session': self.dest_session,  'buffer_size': self.chunk_size, 'resource_kwargs': { 'endpoint_url': self.dest_endpoint}, 'multipart_upload': multipart}, ignore_ext=True) as s3_destination:
        for chunk in self.read_in_chunks(s3_source):
           s3_destination.write(chunk)
    '''
    
    #Temporary code, While concurrent S3 session bug exists
    
    #Read from source S3; Write to temporary file
    with open(source_s3_uri, 'rb', transport_params={'session': self.src_session,  'buffer_size': self.chunk_size, 'resource_kwargs': { 'endpoint_url': self.src_endpoint}}, ignore_ext=True) as s3_source:
      with open(self.TEMP_FILENAME, 'wb') as fout:
        for chunk in self.read_in_chunks(s3_source):
           fout.write(chunk)
           
    #Read from temporary file; Write to destination S3.
    with open(self.TEMP_FILENAME, 'rb') as fin:
      with open(dest_s3_uri, 'wb', transport_params={'session': self.dest_session,  'buffer_size': self.chunk_size, 'resource_kwargs': { 'endpoint_url': self.dest_endpoint}, 'multipart_upload': multipart}, ignore_ext=True) as s3_destination:
        for chunk in self.read_in_chunks(fin):
           s3_destination.write(chunk)
           
    os.remove(self.TEMP_FILENAME)

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

  def rsync(self, src_dir, dest_bucket, backup_prefix ):
    '''
      Copy all objects in the source S3 bucket to the destination S3 bucket
      Prefixing the destination key with the source bucket name.
    
      src_bucket: bucket name
      dest_bucket: bucket name
    '''
    #Preseed the destination bucket object keys dictionary with the Object ETags.
    dest_keys = {}
    for r in self.bucket_ls(s3=self.dest_connection, bucket=dest_bucket, prefix='{}/'.format(backup_prefix)):
      key = re.sub(r"^{}/".format(src_bucket), '', r['Key'])
      dest_keys[key] = r['ETag']
      if self.debug >= 3: print('DEST: ', r['Key'], ' ', r['ETag'], ' ', r['Size']) 

    #Get the object headers from the source S3, and see if these need to be copied.
    for r in self.bucket_ls(s3=self.src_connection, bucket=src_bucket):
      if r['Key'] in dest_keys:
        if dest_keys[r['Key']] == r['ETag']:
          if self.debug >= 2: print('Exists:  ', src_bucket, "/", r['Key'], ' ', r['ETag'], ' ', r['Size'])
        else: 
          #Have the object, but the ETag differs, so we need to copy, then deleted the last version
          if self.debug >= 1: print('Mismatched: ', src_bucket, "/", r['Key'], ' ', r['ETag'], ' ', r['Size'], dest_keys[r['Key']])
          if self.update: 
            self.s3copys3(src_bucket=src_bucket, dest_bucket=dest_bucket, key=r['Key'], size=r['Size'], disable_multipart = ('-' not in r['ETag']) )
            #Delete previous object, if store has versioning on.
        dest_keys[r['Key']] = None  #So we know we have this one in the source.
      else:
        if self.debug >= 1: print('Copying: ', src_bucket, "/", r['Key'], ' ', r['ETag'], ' ', r['Size']) 
        if self.update: self.s3copys3(src_bucket=src_bucket, dest_bucket=dest_bucket, key=r['Key'], size=r['Size'], disable_multipart = ('-' not in r['ETag']))

    for r in dest_keys:
      if dest_keys[r] is not None:
        if self.debug >= 2: print('DELETED: ', r)
        #Might want to age out the deleted ones.

def parse_args():
  parser = argparse.ArgumentParser(description='rsync from source s3 bucket to dest s3 bucket')
  parser.add_argument('-?', action='help', default=argparse.SUPPRESS, help=argparse._('show this help message and exit'))
  parser.add_argument('-d', '--debug', dest='debug_lvl', default=0,  help="0: Off, 1: Copy/Mismatch messages, 2: Exists messages, 3: Dest ls", type=int)
  parser.add_argument('-n', '--no_rsync', dest='no_rsync', action='store_true', help='Use with Debugging. Default is to perform s3 rsync')
  parser.add_argument('-c', '--conf', dest='conf_file', help='Specify JSON conf file for source and destination')
  parser.add_argument('-a', '--auth', dest='auth_file', help='Specify JSON auth file containing s3 keys')
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
    raise sys.exit(1)

def main():
  args = parse_args()
  auth = json_load(args.auth_file)
  conf = json_load(args.conf_file)

  if 'chunk_size' not in conf or type(conf['chunk_size']): conf['chunk_size'] = 1073741824 #1G

  backup_directory = conf['backup']['directory']
  file_pattern = conf['backup']['file_pattern']

  if backup_directory is None or backup_directory == '' or file_pattern is None or file_pattern == ''
    print("Need to define backup directory and file_pattern in conf file")
    sys.exit(1)
  
  #Change to the backup directory, so we don't have to concatentate the dir with the file names.
  os.chdir(backup_directory)

  #Get a directory list, in reverse alphabetical order, where the filename matches our backup filename pattern.
  #Reverse order gives the newest files first, as there are YYYY-MM-DD dates in the names.
  listing = sorted(glob.glob(file_pattern), reverse=True)

  #Iterate over a copy of the directory listing.
  #Remove any zero length backup files.
  for f in listing[:]:
    if os.path.getsize(f) == 0:
      print("os.remove({})".format(f))
      listing.remove(f)
      os.remove(f)

  #Check the most recend 7 files have been rsync'd to the S3 store
  for f in listing[None:7]:
    print("s3rsync({})".format(f))
  
  #Keep only the last 7 versions on the local disk and in the S3 store
  for f in  listing[7:None]:
    print("s3rsync_delete({})".format(f))
    print("os.remove({})".format(f))
    os.remove(f)
