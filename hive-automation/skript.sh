dir=$(pwd)
s=$dir/tmp/input

echo "$dir"
echo "$s"

if [ "$s" = "$dir" ]; then
        echo "Ok"

else
if [ -d  "$dir/tmp" ] 
then
    echo "Directory $dir/tmp exists." 
    if [ -d  "$dir/tmp/input" ] 
        then
            echo "Directory $dir/tmp/input exists." 
        else
            echo "Error: Directory $dir/tmp/input does not exists."
            mkdir $dir/tmp/input
    fi
    if [ -d  "$dir/tmp/output" ] 
        then
        echo "Directory $dir/tmp exists." 
        else
	echo "Error: Directory $dir/tmp/output does not exists."
        mkdir $dir/tmp/output
    fi
    if [ -d  "$dir/tmp/hql-input" ] 
        then
	echo "Directory $dir/tmp/hql-input exists." 
    else
        echo "Error: Directory $dir/tmp/hql-input does not exists."
        mkdir $dir/tmp/hql-input
    fi
    if [ -d  "$dir/tmp/hql-output" ] 
        then
        echo "Directory $dir/tmp/hql-output exists." 
    else
        echo "Error: Directory $dir/tmp/hql-output does not exists."
        mkdir $dir/tmp/hql-output
    fi

else
    echo "Error: Directory $dir/tmp does not exists."
    mkdir $dir/tmp
    if [ -d  "$dir/tmp/input" ] 
        then
            echo "Directory $dir/tmp/input exists." 
        else
            echo "Error: Directory $dir/tmp/input does not exists."
           mkdir $dir/tmp/input
    fi
    if [ -d  "$dir/tmp/output" ] 
        then
	echo "Directory $dir/tmp exists." 
        else
	echo "Error: Directory $dir/tmp/output does not exists."
        mkdir $dir/tmp/output
    fi
    if [ -d  "$dir/tmp/hql-input" ] 
        then
	echo "Directory $dir/tmp/hql-input exists." 
    else
        echo "Error: Directory $dir/tmp/hql-input does not exists."
        mkdir $dir/tmp/hql-input
    fi
    if [ -d  "$dir/tmp/hql-output" ] 
        then
	echo "Directory $dir/tmp/hql-output exists." 
    else
        echo "Error: Directory $dir/tmp/hql-output does not exists."
        mkdir $dir/tmp/hql-output
    fi
cd /tmp/input/
exec bash
fi
cd /tmp/input/
exec bash
fi
cd /tmp/input/
exec bash
echo "You are not in the $dir/tmp/input folder?"
cd /tmp/input/
exec bash
# Running the main part of the script

## This script allow you to run the SQL file and get the output query

## Input parameters and definition of the log file are set here

log=$dir/tmp/skriptlog.txt
printf "Log file: " > $log
#date >> $log

## The script starts by copying all the files from the given S3 bucket
echo "The beginnig of the script!" >> $log
rsync -r $dir/root/tmp/hql-input/ $dir/tmp/input/
echo "Copy all fails from S3 bucket!" >> $log
echo "Copy all fails from S3 bucket!"
for FILE in *
do
## The date of th current file is added from the S3 bucket
##        curentDA=$(aws s3 ls s3://hql-input/$FILE | sort | awk '{print $1$2}')
        curentDA=$(date -r $dir/tmp/hql-input/$FILE +"%Y-%m-%d%H:%M:%S")
        touch -d "$curentDA" $dir/tmp/input/$FILE
        echo "Assign a date to the current file from the S3 bucket" "${FILE%.hql}.csv" "$curentDA" >> $log
        echo "Assign a date to the current file from the S3 bucket" "${FILE%.hql}.csv" "$curentDA"

## Check if we have the output query of the current file and if it is not there we process the 
## file and place the output query in the local output folder and the S3 bucket folder
        if [ ! -e $dir/tmp/output/"${FILE%.hql}.csv" ]; then
          echo "Processing $FILE file..." >> $log
          echo "Processing $FILE file..."
          hive -f $FILE > $dir/tmp/output/"${FILE%.hql}.csv"
          cp $dir/tmp/output/"${FILE%.hql}.csv" $dir/tmp/hql-output/"${FILE%.hql}.csv"
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
rsync -r $dir/tmp/hql-input/ $dir/tmp/input/
echo "Copying all new fails from S3 bucket!" >> $log
echo "Copying all new fails from S3 bucket!"

## Check files from S3 bucket and /tmp/input/ folder for new files
  for FILE in *
  do
##        curentAWSDate=$(aws s3 ls s3://hql-input/$FILE | sort | awk '{print $1$2}')
        curentAWSDate=$(date -r $dir/tmp/hql-input/$FILE +"%Y-%m-%d%H:%M:%S")
        echo "Copy from S3 bucket the date of " "$FILE" "$curentAWSDate"  >> $log
        echo "Copy from S3 bucket the date of " "$FILE" "$curentAWSDate"

## Check if we have the output query of the current file and if it is not there we process the 
## file and place the output query in the local output folder and the S3 bucket folder
        if [ ! -e $dir/tmp/output/"${FILE%.hql}.csv" ]; then
          echo "Processing $FILE file..." >> $log
          echo "Processing $FILE file..."
          hive -f $FILE > $dir/tmp/output/"${FILE%.hql}.csv"
          cp $dir/tmp/output/"${FILE%.hql}.csv" $dir/tmp/hql-output/"${FILE%.hql}.csv"
          echo "Copy output query to S3 bucket!" >> $log
          echo "Copy output query to S3 bucket!"
        fi

        echo "File $FILE it already exists!" >> $log
        echo "File $FILE it already exists!"
        echo "Wait!"
        copyDate=$(date -r $dir/tmp/input/$FILE +"%Y-%m-%d%H:%M:%S")
        echo "Copy from Input folder the date of" "$FILE" "$copyDate"

## If the file dates are different, they are deleted from the local folders
           if [[ "$copyDate" < "$curentAWSDate" ]]; then
              echo "The dates are different!" >> $log
              echo "The dates are different!"
              rm $dir/tmp/input/$FILE
              rm $dir/tmp/output/"${FILE%.hql}.csv"
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

# End of the main part of the script


exit

