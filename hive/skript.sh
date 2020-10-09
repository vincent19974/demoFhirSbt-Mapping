echo "The beginnig of the script!"
echo "Copy a file from one S3 bucket to another S3 bucket!"
currentDate=$(aws s3 ls s3://sasabucket1/proba1.hql | sort | tail -n 1 | awk '{print $1}')
echo "The current Date is: $currentDate"
for (( ; ; ))
do
echo "Infinite loops [hit CTRL+C] to stop"
newDate=$(aws s3 ls s3://sasabucket1/proba1.hql | sort | tail -n 1 | awk '{print $1}')
echo "The new date is: $newDate"
if [ $currentDate != $newDate ]
then
		echo "File date changed!"
		hive -f proba1.hql
		echo "SQL file started!"
		/home/ec2-user/skripta.sh > hive_output.txt
		aws s3 cp hive_output.txt s3://sasabucket1/
		git init
		git add hive_output.txt
		git commit -m hive_output.txt
		git remote add primer https://github.com/data-bar/aws/blob/master/hive-automation/
		git remote -v
		git push primer master
		sleep 1m
else
		echo "The file is not new yet!"
		sleep 1m
fi
done
echo "End of scripte!"
exit
