import boto3
from pprint import pprint

old_bucket_name = 'sasabucket1'
old_prefix = 'hql-input/'
new_bucket_name = 'sasabucket1'
new_prefix = 'input/'
s3 = boto3.resource('s3')
old_bucket = s3.Bucket(old_bucket_name)
new_bucket = s3.Bucket(new_bucket_name)

for obj in old_bucket.objects.filter(Prefix=old_prefix):
    old_source = { 'Bucket': old_bucket_name,
                   'Key': obj.key,
                   'LastModified': obj.last_modified}
    pprint(obj.last_modified)
    pprint('------------------------------------')
    pprint(old_source['LastModified'])
    pprint('***************************')
# get date of old_source
    for obj2 in new_bucket.objects.filter(Prefix=new_prefix):
        new_source = { 'Bucket': new_bucket_name,
                        'Key': obj2.key,
                        'LastModified': obj2.last_modified}
        pprint(obj2.last_modified)
        pprint('++++++++++++++++++++++++++')
        if obj.last_modified == obj2.last_modified:
                pprint("Datum je isti!")
        else :
              	pprint("Nije isti datum!")
