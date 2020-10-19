
echo "The beginnig of the script!"
aws s3 sync s3://sasabucket1/ /home/ec2-user/tmp/input/
echo "Copy all fails from S3 bucket!"
for FILE in *
do
curentDA=$(aws s3 ls s3://sasabucket1/$FILE | sort | awk '{print $1$2}')
touch -d "$curentDA" /home/ec2-user/tmp/input/$FILE
        echo "Test" "${FILE%.hql}.csv" "$curentDA"
        if [ ! -e /home/ec2-user/tmp/output/"${FILE%.hql}.csv" ]; then
          echo "Processing $FILE file..."
          echo "$FILE"
          hive -f $FILE > /home/ec2-user/tmp/output/"${FILE%.hql}.csv"
          aws s3 cp /home/ec2-user/tmp/output/"${FILE%.hql}.csv" s3://outputhql/"${FILE%.hql}.csv"
          echo "Next file!"
        fi
	echo "File $FILE it already exists!"
done
echo "The beginnig of the loop!"
for (( ; ; ))
do 
echo "Infinite loops [hit CTRL+C] to stop"
sleep 5s
aws s3 sync s3://sasabucket1/ /home/ec2-user/tmp/input/
echo "Copy all fails from S3 bucket!"
  for FILE in *
  do
curentAWSDate=$(aws s3 ls s3://sasabucket1/$FILE | sort | awk '{print $1$2}')
        echo "Test 2" "$FILE" "$curentAWSDate"
        if [ ! -e /home/ec2-user/tmp/output/"${FILE%.hql}.csv" ]; then
          echo "Processing $FILE file..."
          echo "$FILE"
        hive -f $FILE > /home/ec2-user/tmp/output/"${FILE%.hql}.csv"
          aws s3 cp /home/ec2-user/tmp/output/"${FILE%.hql}.csv" s3://outputhql/"${FILE%.hql}.csv"
          echo "Next file!"
        fi
echo "File $FILE it already exists!"
        echo "Wait one minute!"
copyDate=$(date -r /home/ec2-user/tmp/input/$FILE "+%Y-%m-%d%H:%M:%S")
        echo "Test 2" "$FILE" "$curentAWSDate" "$copyDate"
   if [[ "$copyDate" < "$curentAWSDate" ]]; then
      echo "The dates are different!"
        rm /home/ec2-user/tmp/input/$FILE
        rm /home/ec2-user/tmp/output/"${FILE%.hql}.csv"
        fi
        echo "The dates are not different!"
        sleep 5s
  done
done
echo "End of scripte!"
exit
