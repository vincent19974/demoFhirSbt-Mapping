import json
import boto3

def lambda_handler(event, context):
    s3_resource = boto3.resource('s3')
    mybucket = s3_resource.Bucket('sasabucket1')
    
    
    get_last_modified = lambda obj: int(obj.last_modified.strftime('%s'))
    objs  = [file for file in mybucket.objects.all()]
    objs = [obj for obj in sorted(objs, key=get_last_modified)]
    last_added = objs[-1].key
    
    sns_client = boto3.client('sns')
    
    sns_client.publish(TopicArn="arn:aws:sns:us-east-1:723293022411:S3_Notifications",Message="New file in s3 bucket:{}".format(last_added),Subject="New file uploaded")