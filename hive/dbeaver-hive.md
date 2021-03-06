# Connect to Hive on EMR using DBeaver

1. Install DBeaver-a (Community Edition: `https://dbeaver.io/download/`)
2. Start Hive Server on EMR: `hive --service hiveserver2 &`
3. Take IP address of EMR: `https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1`
4. Setup DBeaver to point to Hive
5. Download (automatically) Hive drivers for DBeaver 
6. Enter IP address and click Finish/Connect:
![alt DBeaver](https://github.com/data-bar/aws/blob/master/hive/dbeaver_files/DBeaver-connection-window.png)

7. See the Result - Tables and Records:

![alt DBeaver](https://github.com/data-bar/aws/blob/master/hive/dbeaver_files/DBeaver-result-window.png)
