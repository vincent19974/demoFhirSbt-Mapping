echo "The beginnig of the script!"
echo "Ente the SQL file name:"
read sqlfile
echo "Copy the current date of the given SQL file from S3 bucket!"
currentDate=$(aws s3 ls s3://sasabucket1/$sqlfile | sort | tail -n 1 | awk '{print $1}')
echo "The current Date is: $currentDate"
echo "Copy the current SQL file:"
aws s3 cp s3://sasabucket1/$sqlfile  $sqlfile
echo "SQL file started!"
hive -f $sqlfile
/home/ec2-user/skript.sh > hive_output.txt
aws s3 cp hive_output.txt s3://sasabucket1/
git init
git add hive_output.txt
git commit -m hive_output.txt
git remote add hive_output.txt  https://github.com/data-bar/aws/tree/master/hive-automation/
git remote -v
git push hive_output.txt master
sleep 1m
for (( ; ; ))
do 
echo "Infinite loops [hit CTRL+C] to stop"
newDate=$(aws s3 ls s3://sasabucket1/$sqlfile | sort | tail -n 1 | awk '{print $1}')
echo "The new date is: $newDate"
if [ "$currentDate" == "$newDate" ]
then
		echo "The date of the file has not been changed yet!"
		sleep 1m
else
		echo "File date changed!"
		hive -f "$sqlfile"
		echo "SQL file started!"
		/home/ec2-user/skript.sh > hive_output.txt
		aws s3 cp hive_output.txt s3://sasabucket1/
		git init
		git add hive_output.txt
		git commit -m hive_output.txt
		git remote add hive_output.txt https://github.com/data-bar/aws/blob/master/hive-automation/
		git remote -v
		git push hive_output.txt master
		sleep 1m
fi
done
echo "End of scripte!"
exit
