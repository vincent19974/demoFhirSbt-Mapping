# Databar

#

# Synchronization of two folders

import boto3
from pprint import pprint

import boto3
old_bucket_name = 'sasabucket1'
old_prefix = 'hql-input/'
new_bucket_name = 'sasabucket1'
new_prefix = 'input/'
s3 = boto3.resource('s3')
old_bucket = s3.Bucket(old_bucket_name)
new_bucket = s3.Bucket(new_bucket_name)

for obj in old_bucket.objects.filter(Prefix=old_prefix):
    old_source = { 'Bucket': old_bucket_name,
                   'Key': obj.key}
    # replace the prefix
    new_key = obj.key.replace(old_prefix, new_prefix, 1)
    new_obj = new_bucket.Object(new_key)
    new_obj.copy(old_source)
