dir=$(pwd)
s=/home/hadoop/tmp/input

echo "$dir"
echo "$s"

if [ "$s" = "$dir" ]; then
        echo "Ok"


## This script allow you to run the SQL file and get the output query

## Input parameters and definition of the log file are set here

log=/home/hadoop/skriptlog.txt
printf "Log file: " > $log
#date >> $log

## The script starts by copying all the files from the given S3 bucket
echo "The beginnig of the script!" >> $log
aws s3 sync s3://hql-input/ /home/hadoop/tmp/input/
echo "Copy all fails from S3 bucket!" >> $log
echo "Copy all fails from S3 bucket!"
for FILE in *
do
## The date of th current file is added from the S3 bucket
        curentDA=$(aws s3 ls s3://hql-input/$FILE | sort | awk '{print $1$2}')
        touch -d "$curentDA" /home/hadoop/tmp/input/$FILE
        echo "Assign a date to the current file from the S3 bucket" "${FILE%.hql}.csv" "$curentDA" >> $log
        echo "Assign a date to the current file from the S3 bucket" "${FILE%.hql}.csv" "$curentDA"

## Check if we have the output query of the current file and if it is not there we process the 
## file and place the output query in the local output folder and the S3 bucket folder
        if [ ! -e /home/hadoop/tmp/output/"${FILE%.hql}.csv" ]; then
          echo "Processing $FILE file..." >> $log
          echo "Processing $FILE file..."
          hive -f $FILE > /home/hadoop/tmp/output/"${FILE%.hql}.csv"
          aws s3 cp /home/hadoop/tmp/output/"${FILE%.hql}.csv" s3://hql-output/"${FILE%.hql}.csv"
          echo "Copy output query to S3 bucket!" >> $log
          echo "Copy output query to S3 bucket!"
        fi

        echo "File $FILE it already exists!" >> $log
done  

echo "The beginnig of the loop!" >> $log
echo "The beginnig of the loop!"


## Starting an infinite loop to check the modified files on a given S3 bucket
for (( ; ; ))
do 
echo "Infinite loops [hit CTRL+C] to stop" >> $log
echo "Infinite loops [hit CTRL+C] to stop"
sleep 5s
aws s3 sync s3://hql-input/ /home/hadoop/tmp/input/
echo "Copying all new fails from S3 bucket!" >> $log
echo "Copying all new fails from S3 bucket!"

## Check files from S3 bucket and /tmp/input/ folder for new files
  for FILE in *
  do
        curentAWSDate=$(aws s3 ls s3://hql-input/$FILE | sort | awk '{print $1$2}')
        echo "Copy from S3 bucket the date of " "$FILE" "$curentAWSDate"  >> $log
        echo "Copy from S3 bucket the date of " "$FILE" "$curentAWSDate"

## Check if we have the output query of the current file and if it is not there we process the 
## file and place the output query in the local output folder and the S3 bucket folder
        if [ ! -e /home/hadoop/tmp/output/"${FILE%.hql}.csv" ]; then
          echo "Processing $FILE file..." >> $log
          echo "Processing $FILE file..."
          hive -f $FILE > /home/hadoop/tmp/output/"${FILE%.hql}.csv"
          aws s3 cp /home/hadoop/tmp/output/"${FILE%.hql}.csv" s3://hql-output/"${FILE%.hql}.csv"
          echo "Copy output query to S3 bucket!" >> $log
          echo "Copy output query to S3 bucket!"
        fi

        echo "File $FILE it already exists!" >> $log
        echo "File $FILE it already exists!"
        echo "Wait!"
        copyDate=$(date -r /home/hadoop/tmp/input/$FILE +"%Y-%m-%d%H:%M:%S")
        echo "Copy from Input folder the date of" "$FILE" "$copyDate"

## If the file dates are different, they are deleted from the local folders
           if [[ "$copyDate" < "$curentAWSDate" ]]; then
              echo "The dates are different!" >> $log
              echo "The dates are different!"
              rm /home/hadoop/tmp/input/$FILE
              rm /home/hadoop/tmp/output/"${FILE%.hql}.csv"
              echo "Remove the checked file!" >> $log
              echo "Remove the checked file!"
           fi

        echo "The dates are not different!" >> $log
        echo "The dates are not different!" 
        sleep 5s

  done
done
echo "End of scripte!"
fi
echo "You are not in the /tmp/input folder?"
exit

