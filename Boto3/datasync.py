import boto3

s3 = boto3.resource('s3')
bucket = s3.Bucket('my-bucket')
for obj in bucket.objects.all():
    print(obj.key)




import boto3

client = boto3.client('datasync')


import boto3

client = boto3.client('s3')
paginator = client.get_paginator('list_objects')
result = paginator.paginate(Bucket='my-bucket', Delimiter='/')
for prefix in result.search('CommonPrefixes'):
    print(prefix.get('Prefix'))