#!/user/bin/python

from cassandra.cluster import Cluster
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import json
import boto3
from pyhive import hive
from io import StringIO # python3; python2: BytesIO


# Key Space Connection
ssl_context = SSLContext(PROTOCOL_TLSv1_2 )



session = cluster.connect()

s3_resource = boto3.resource('s3')


 
d = json.loads(s)
with open('/tmp/hppp.json', 'w') as json_file:
    json.dump(d, json_file)




with open('/tmp/hppp.json') as f:
    k_v = json.load(f)

list_k_v = k_v['data']
df_j = pd.DataFrame(list_k_v)


for i,row in df_j.iterrows():
    h_list = [str(x) for x in row['hcolist']]
    # Keyspace
    print(h_list)
    df_e = pd.DataFrame()
    for x in h_list:
        str_x = str(x)
        sql_query = "select * from {db}.{table} where {key1} = '{l2}'".format(db=row['keyspaceDb'],table=row['keyspaceTable'],key1=row['keyCol'],l2=str_x)
        print(sql_query)
        df = pd.DataFrame(list(session.execute(sql_query)))
        df_e = df_e.append(df)
    #sql_query = "select * from {db}.{table} where {key1} in {l2}".format(db=row['keyspaceDb'],table=row['keyspaceTable'],key1=row['keyCol'],l2=tuple(h_list))
    #print (sql_query)
    #df = pd.DataFrame(list(session.execute(sql_query)))
    df_ksc = df_e.groupby(row['keyCol']).size().reset_index(name='counts')
    print('Key Space Fetch is Done')
    # writing key space to S3
    csv_buffer = StringIO()
    df_ksc.to_csv(csv_buffer)
    #object = s3.Object('p360-poc-s3-log-storage', 'elb-logs-1/df2.txt')
    #object.put(Body=csv_buffer.getvalue())
    #s3_resource.Object(bucket, 'NetWrkMeas-Cust/df_ksc.csv').put(Body=csv_buffer.getvalue())
    #Hive
    conn = hive.Connection(host='localhost', port=10000,  database=row['athenaDb'])
    sql_query2 = "select * from {db}.{table} where {key1} in {l2}".format(db=row['athenaDb'],table=row['athenaTable'],key1=row['keyCol'],l2=tuple(h_list))
    df_a = pd.read_sql(sql_query2, conn)
    print(df_a.columns)
    df_a.columns = [x.replace(row['athenaTable']+'.','') for x in df_a.columns.values]
    print(df_a.columns)
    df_atc = df_a.groupby(row['keyCol']).size().reset_index(name='counts')
    # writing key space to S3
    #csv_buffer = StringIO()
    #df_atc.to_csv(csv_buffer)
    #s3_resource.Object(bucket, 'NetWrkMeas-Cust/df_atc.csv').put(Body=csv_buffer.getvalue())
    print('Hive fetch is done')
    #Comparison
    df_merge = df_atc.merge(df_ksc, how='left',on=[row['keyCol'],'counts'],indicator=True)
    print(df_merge.columns)
    df_merge_filter = df_merge[df_merge['_merge']=='left_only']
    df_final = df_merge_filter.merge(df_ksc,how='inner',on=row['keyCol'])
    # writing key space to S3
    csv_buffer = StringIO()
    df_final.to_csv(csv_buffer)
    bucket = 'p360-poc-s3-log-storage'
    s3_loc = "{}_mismatch_counts.csv".format(row['keyspaceTable'])
    s3_resource.Object(bucket, 'elb-logs-1/'+ s3_loc).put(Body=csv_buffer.getvalue())



 