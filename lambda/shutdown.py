import boto3

RunningInstances = []

#Settings
region = 'us-east-1'
ec2 = boto3.client('ec2', region_name=region)
ec2_inst = boto3.resource("ec2")

def lambda_handler(event, context):
    
    filters = [
        {
            "Name": "tag:shutdown",
            "Values": ["yes"]
        }
    ]
    
    instances = ec2_inst.instances.filter(Filters = filters)
   
    for instance in instances:
        RunningInstances.append(instance.id)
           
    #print(RunningInstances)
    ec2.stop_instances(InstanceIds=RunningInstances)