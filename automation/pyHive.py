from pandas import DataFrame
from pyhive import hive
import boto3
import json

s3 = boto3.client('s3')
s3.download_file('aws-logs-723293022411-us-east-1', 'bootstrap-test/hiveConf.json', 'hiveConf.json')

f = open("hiveConf.json", "r")

jsonFile = json.load(f)




cursor = hive.connect(jsonFile["hiveConf"]["host"]).cursor()
cursor.execute(jsonFile["hiveConf"]["execute1"])
cursor.execute(jsonFile["hiveConf"]["execute2"])
cursor.execute(jsonFile["hiveConf"]["execute3"])
df = DataFrame(cursor.fetchall())
df.to_csv(jsonFile["hiveConf"]["saveTo"])