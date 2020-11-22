import boto3
from pprint import pprint

s3 = boto3.client('s3')
bucket = 'sasabucket1'
prefix = 'hql-input/'
for obj in s3.list_objects_v2(Bucket=bucket, Prefix=prefix)['Contents']:
    pprint(obj['Key'])
    pprint(obj['LastModified'])
    pprint("==========================")
