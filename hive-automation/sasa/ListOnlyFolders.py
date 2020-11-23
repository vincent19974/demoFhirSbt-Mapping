import os
import boto3
from pprint import pprint


client = boto3.client('s3')
paginator = client.get_paginator('list_objects')
result = paginator.paginate(Bucket='sasabucket1', Delimiter='/')
for prefix in result.search('CommonPrefixes'):
    pprint(prefix.get('Prefix'))
