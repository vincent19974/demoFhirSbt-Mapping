import boto3
import pandas as pd 
import numpy as np
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

client = boto3.client("sagemaker")
#s3 = boto3.client('s3')

def lambda_handler(event, context):
    # create pandas dataframe
    names =  ["name", "start", "processing", "end"]
    df = pd.DataFrame(np.zeros((100, 4)), columns=names)
    logger.info("Hello from pandas")
    
    # do some pandas processing
    # listing files from bucket
    #files = s3.list_objects(Bucket='radionica', Prefix='proba')['Contents']
    
    # list all sagemaker notebook instances
    notebook_instances = client.list_notebook_instances()
    for instance in notebook_instances["NotebookInstances"]:
        instance_name = instance["NotebookInstanceName"]
        logger.info(f"instance {instance_name} is in Service")
        # get ARN of instance
        instance_arn = instance["NotebookInstanceArn"]
        # list tags
        instance_tags = client.list_tags(ResourceArn=instance_arn)
        # start/stop instances based on event on/off
        # can trigger by cloudwatch, cron jobs
        for tag in instance_tags["Tags"]:
            if (tag["Key"] == "Pandas" and tag["Value"] =="Yes"):
                logger.info(f"instance {instance} is Pandas")
                if (event["event"] == "On"):
                    response = client.start_notebook_instance(NotebookInstanceName=instance_name)
                    logger.info(f"instance {instance_name} was started")
                elif (event["event"] == "Off"):
                    response = client.stop_notebook_instance(NotebookInstanceName=instance_name)
                    logger.info(f"instance {instance_name} was stopped")
            # else:
            #     logger.info(f"instance {instance_name} Not is Pandas")

    return "Test pass"

