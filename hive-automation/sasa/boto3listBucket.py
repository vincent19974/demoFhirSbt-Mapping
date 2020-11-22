import os
import boto3
import subprocess
from pprint import pprint


## The script starts by copying all the files from the given S3 bucket
subprocess.run(['aws', 's3', 'sync', 's3://sasabucket1/hql-input', 's3://sasabucket1/input'])





s3 = boto3.resource('s3')
my_bucket = s3.Bucket('sasabucket1')
for my_bucket_object in my_bucket.objects.all():
    pprint(my_bucket_object)

