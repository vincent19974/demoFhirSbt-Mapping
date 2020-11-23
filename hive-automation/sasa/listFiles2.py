import os
import boto3
from pprint import pprint


s3 = boto3.resource('s3')
bucket = s3.Bucket('sasabucket1')
for obj in bucket.objects.filter(Prefix='hql-input/'):
    pprint(obj.key)
    pprint("==========================")
    pprint('*************************')
    pprint(obj.last_modified)