echo "The beginnig of the script!"
aws s3 sync s3://sasabucket1/ /home/ec2-user/tmp
echo "Copy all fails from S3 bucket!"
for FILE in *
do
   name="${FILE%.hql}.txt"

	echo $name
	for f in /home/ec2-user/out/*
	do
	if [ "${FILE%.hql}.txt" == "$f" ]; then
	  echo "File $name it already exists!"
	else 
	  echo "Processing $FILE file..."
	  echo "$FILE"
#	  hive -f $FILE
	  cat /home/ec2-user/tmp/"$FILE" > "${FILE%.hql}.txt"
	  cp /home/ec2-user/tmp/"${FILE%.hql}.txt" /home/ec2-user/out/"${FILE%.hql}.txt"
	  aws s3 cp /home/ec2-user/tmp/"${FILE%.hql}.txt" s3://outputhql/"${FILE%.hql}.txt"
          echo "Next file!"
	fi
	done
done
echo "The beginnig of the loop!"
for (( ; ; ))
do 
echo "Infinite loops [hit CTRL+C] to stop"
sleep 1m
aws s3 sync s3://sasabucket1/ /home/ec2-user/tmp
echo "Copy all fails from S3 bucket!"
  for FILE in *
  do
name="${FILE%.hql}.txt"
	for f in /home/ec2-user/out/*  
        do
        if [ "${FILE%.hql}.txt" == "$f" ]; then
          echo "File $name it already exists!"
        else 
	  echo "Processing $FILE file..."
	  echo "$FILE"
#	  hive -f $FILE
          cat /home/ec2-user/tmp/"$FILE" > "${FILE%.hql}.txt"
          cp /home/ec2-user/tmp/"${FILE%.hql}.txt" /home/ec2-user/out/"${FILE%.hql}.txt"
          aws s3 cp /home/ec2-user/tmp/"${FILE%.hql}.txt" s3://outputhql/"${FILE%.hql}.txt"
	  echo "Next file!"
	fi
	done
	echo "Wait one minute!"
	sleep 1m
  done
done
echo "End of scripte!"
exit
