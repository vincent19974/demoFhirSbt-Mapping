echo "Pocetak skripte!"

echo "Kopiranje fajla sa nekog S3  bucket-a na drugi bucket."

aws s3 cp s3://sasabucket1/proba2.hql s3://sasabucket1/sasaf1/

sleep 1m

echo "Prosao je 1 minut"


aws s3 rm s3://sasabucket1/sasaf1/proba2.hql

echo "Obrisan je fajl"



echo "kraj skripte!"
exit

