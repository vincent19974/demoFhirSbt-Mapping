## This script allow you to run the SQL file and get the output query

## Input parameters and definition of the log file are set here

mkdir -p /tmp/input
log=skriptlog.txt
printf "Log file: " > $log
date >> $log

## The script starts by copying all the files from the given S3 bucket
echo "The beginnig of the script!" >> $log
aws s3 sync s3://sasabucket1/ /home/ec2-user/tmp/input/
echo "Copy all fails from S3 bucket!" >> $log
for FILE in *
do
## The date of th current file is added from the S3 bucket
	curentDA=$(aws s3 ls s3://sasabucket1/$FILE | sort | awk '{print $1$2}')
	touch -d "$curentDA" /home/ec2-user/tmp/input/$FILE
	echo "Assign a date to the current file from the S3 bucket" "${FILE%.hql}.csv" "$curentDA" >> $log

## Check if we have the output query of the current file and if it is not there we process the 
## file and place the output query in the local output folder and the S3 bucket folder
	if [ ! -e /home/ec2-user/tmp/output/"${FILE%.hql}.csv" ]; then
	  echo "Processing $FILE file..." >> $log
	  hive -f $FILE > /home/ec2-user/tmp/output/"${FILE%.hql}.csv"
	  aws s3 cp /home/ec2-user/tmp/output/"${FILE%.hql}.csv" s3://outputhql/"${FILE%.hql}.csv"
          echo "Copy output query to S3 bucket!" >> $log
	fi

        echo "File $FILE it already exists!" >> $log
done  

echo "The beginnig of the loop!" >> $log

## Starting an infinite loop to check the modified files on a given S3 bucket
for (( ; ; ))
do 
echo "Infinite loops [hit CTRL+C] to stop" >> $log
sleep 5s
aws s3 sync s3://sasabucket1/ /home/ec2-user/tmp/input/
echo "Copying all new fails from S3 bucket!" >> $log

## Check files from S3 bucket and /tmp/input/ folder for new files
  for FILE in *
  do
	curentAWSDate=$(aws s3 ls s3://sasabucket1/$FILE | sort | awk '{print $1$2}')
	echo "Copy from S3 bucket the date of " "$FILE" "$curentAWSDate"  >> $log

## Check if we have the output query of the current file and if it is not there we process the 
## file and place the output query in the local output folder and the S3 bucket folder
	if [ ! -e /home/ec2-user/tmp/output/"${FILE%.hql}.csv" ]; then
	  echo "Processing $FILE file..." >> $log
	  hive -f $FILE > /home/ec2-user/tmp/output/"${FILE%.hql}.csv"
          aws s3 cp /home/ec2-user/tmp/output/"${FILE%.hql}.csv" s3://outputhql/"${FILE%.hql}.csv"
	  echo "Copy output query to S3 bucket!" >> $log
	fi

	echo "File $FILE it already exists!" >> $log
        echo "Wait one minute!"
	copyDate=$(date -r /home/ec2-user/tmp/input/$FILE "+%Y-%m-%d%H:%M:%S")
	echo "Copy from Input folder the date of" "$FILE" "$copyDate"

## If the file dates are different, they are deleted from the local folders
	   if [[ "$copyDate" < "$curentAWSDate" ]]; then
	      echo "The dates are different!" >> $log
	      rm /home/ec2-user/tmp/input/$FILE
	      rm /home/ec2-user/tmp/output/"${FILE%.hql}.csv"
	      echo "Remove the checked file!" >> $log
	   fi

	echo "The dates are not different!" >> $log
	sleep 5s

  done
done
echo "End of scripte!"
exit

