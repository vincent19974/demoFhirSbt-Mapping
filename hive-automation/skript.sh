echo "The beginnig of the script!"
aws s3 sync s3://sasabucket1/ /home/ec2-user/tmp
echo "Copy all fails from S3 bucket!"
for FILE in *
do
  echo "Processing $FILE file..."
	hive -f $FILE
	/home/ec2-user/tmp/$FILE > hive_output.txt
	aws s3 cp hive_output.txt s3://sasabucket1/hive_output.txt
  echo "Next file!"
done
echo "The beginnig of the loop!"
for (( ; ; ))
do 
echo "Infinite loops [hit CTRL+C] to stop"
sleep 1m
aws s3 sync s3://sasabucket1/ /home/ec2-user/tmp
echo "Copy all fails from S# bucket!"
for FILE in *
do
  echo "Processing $FILE file..."
       hive -f $FILE
       /home/ec2-user/tmp/$FILE > hive_output.txt
       aws s3 cp hive_output.txt s3://sasabucket1/hive_output.txt
  echo "Next file!"
done
echo "Wait one minute!"
		sleep 1m
done
echo "End of scripte!"
exit
