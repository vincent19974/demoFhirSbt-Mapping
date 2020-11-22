# Databar

#

# Scroll through the contents of the bouquet


import os
import boto3
from pprint import pprint


s3 = boto3.resource('s3')
my_bucket = s3.Bucket('sasabucket1')
for my_bucket_object in my_bucket.objects.all():
    pprint(my_bucket_object)
    pprint("==========================")


