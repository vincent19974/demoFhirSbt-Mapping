echo "The beginning of the script!" 

echo "Copy a file from one S3 bucket to another S3 bucket." 
currentDate = $(aws s3 ls s3://sasabucket1/proba3.hql | sort | tail -n 1 | awk '{print $1}'`)

for (( ; ; )) 
do 
       echo "Infinite loops [ hit CTRL+C to stop]" 
  	newDate = $(aws s3 ls s3://sasabucket1/proba3.hql | sort | tail -n 1 | awk '{print $1}'`) 
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

