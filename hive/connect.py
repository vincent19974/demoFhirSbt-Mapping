### Installation:
# pip install pyhive
# pip install thrift-sasl

### Connect:
from pyhive import hive
import pandas

conn = hive.Connection(host='3.236.56.144', port=10000,  database='default')

sql = 'select name, count(*) from names group by name'

cursor = conn.cursor()
cursor.execute('SELECT * from names')
print (cursor.fetchall())


df = pandas.read_sql(sql, conn)
print (df)
