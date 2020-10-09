echo "The beginning of the script!" 

echo "Copy a file from one S3 bucket to another S3 bucket." 
aws s3 cp s3://sasabucket1/proba3.hql  proba3.hql
currentDate = $(date +%F -r proba3.hql) 

for (( ; ; )) 
do 
       echo "Infinite loops [ hit CTRL+C to stop]" 
  	  aws s3 cp s3://sasabucket1/proba3.hql  proba3.hql
       newDate = $(date +%F -r proba3.hql) 
       if [ currentDate <> newDate ] 
       then 
               echo "File date changed!" 
               
               
               hive -f proba3.hql 
               echo "SQL file started!" 
               /home/ec2-user/skripta.sh > hive_output.txt 
               aws s3 cp hive_output.txt s3://sasabucket1/ 
               it init
               git add hive_ouptut.txt
               git commit -m hive_output.txt
	             git remote add primer https://github.com/data-bar/aws/blob/master/hive-automation/
               git remote -v
               git push primer master
               sleep 1m
       else 
               echo "The file is not new yet!"
               sleep 1m 

done 

echo "End of script" 

exit

