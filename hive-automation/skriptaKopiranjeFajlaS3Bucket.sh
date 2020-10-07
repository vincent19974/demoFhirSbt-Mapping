echo "Pocetak skripte!"

echo "Kopiranje fajla sa nekog S3  bucket-a na drugi bucket."

aws s3 cp s3://sasabucket1/proba2.hql  proba3.hql

sleep 5s

hive -f proba3.hql

echo "Startovan fajl."


./skripta.sh > hive_output.txt
/home/ec2-user/skripta.sh > hive_output.txt


aws s3 cp hive_output.txt s3://sasabucket1/


echo "Uploadovan je fajl na sasabucket1."

# git add hive_ouptut.txt
# git commit 
# git push https://github.com/data-bar/aws/blob/master/hive-automation/

echo "kraj skripte!"
exit

