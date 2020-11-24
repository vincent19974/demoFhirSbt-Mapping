import boto3

RunningInstances = []
keep_spining_instances = []
shutdown_instances = []
#Settings
region = 'us-east-1'
ec2 = boto3.client('ec2', region_name=region)
ec2_inst = boto3.resource("ec2")

def lambda_handler(event, context):
    
    filters = [
        {
            "Name": "tag:shutdown",
            "Values": ["no"]
        }
    ]
    
    instances = ec2_inst.instances.filter()
   
    for instance in instances:
        RunningInstances.append(instance.id)
        
    keep_instances = ec2_inst.instances.filter(Filters = filters)
   
    for instance in keep_instances:
        keep_spining_instances.append(instance.id)
        
    for i in RunningInstances:
        if i not in keep_spining_instances:
            shutdown_instances.append(i)
    
    print("Keep:")
    print(keep_spining_instances)
    
    print("Shutdown:")
    print(shutdown_instances)
           
    print("Runing:")
    print(RunningInstances)
    
    ec2.stop_instances(InstanceIds=shutdown_instances)
    
    #tear down
    RunningInstances.clear()
    keep_spining_instances.clear()
    shutdown_instances.clear()
