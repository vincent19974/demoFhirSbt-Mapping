echo "The beginning of the script!" 

echo "Copy a file from one S3 bucket to another S3 bucket." 

currentDate = $(date +%F -r proba3.hql) 

for (( ; ; )) 
do 
       echo "Infinite loops [ hit CTRL+C to stop]" 
  	  aws s3 cp s3://sasabucket1/proba3.hql  proba3.hql
       newDate = $(date +%F -r proba3.hql) 
       if [ currentDate <> newDate ] 
       then 
               echo "File date changed!" 
               
               sleep 5s 
               hive -f proba3.hql 
               echo "SQL file started!" 
               /home/ec2-user/skripta.sh > hive_output.txt 
               aws s3 cp hive_output.txt s3://sasabucket1/ 
       else 
               echo "The file is not new yet!"

done 

echo "End of script" 

exit

