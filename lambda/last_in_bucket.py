import json
import boto3

def lambda_handler(event, context):
    
    ##Set up##
    bucket_name = 'outputhql'
    topic_arn = "arn:aws:sns:us-east-1:723293022411:S3_Notifications"
    sns_client = boto3.client('sns')
    s3_resource = boto3.resource('s3')
    mybucket = s3_resource.Bucket(bucket_name)

    ##Pull last file##
    get_last_modified = lambda obj: int(obj.last_modified.strftime('%s'))
    objs  = [file for file in mybucket.objects.all()]
    objs = [obj for obj in sorted(objs, key=get_last_modified)]
    last_added = objs[-1].key
    
    ##Msg setup##
    msg = "New file added in s3 \n File name: {} \n S3 bucket : {}".format(last_added,bucket_name)
    subj = "New file in s3 bucket"

    ##Send msg##
    sns_client.publish(TopicArn=topic_arn,Message=msg,Subject=subj)