import os
import boto3
from pprint import pprint


s3 = boto3.resource('s3')
my_bucket = s3.Bucket('sasabucket1')
for my_bucket_object in my_bucket.objects.all():
    pprint(my_bucket_object)


s3 = boto3.client('s3')
bucket = 'sasabucket1'
prefix = 'hql-input/'
for obj in s3.list_objects_v2(Bucket=bucket, Prefix=prefix)['Contents']:
    pprint(obj['Key'])
    pprint("==========================")





