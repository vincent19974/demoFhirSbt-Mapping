### Installation:
# pip install pyhive
# pip install thrift-sasl

### Connect:
from pyhive import hive
import pandas

conn = hive.Connection(host='3.236.56.144', port=10000,  database='default')
cursor = conn.cursor()

sql = 'select * from names'

cursor.execute(sql)
print (cursor.fetchall())


df = pandas.read_sql(sql, conn)
print (df)
