aws emr create-cluster --name "TerminationProtectedCluster" --release-label emr-6.1.0  --applications Name=Hadoop Name=Hive Name=Pig Name=Tez Name=Hue  Name=JupyterHub Name=Spark   --ec2-attributes InstanceProfile=ec2_role_ssm,KeyName=emrkey --instance-type m1.large  --instance-count 1 --termination-protected  --service-role EMR_DefaultRole --ebs-root-volume-size 30 --log-uri s3://emr-TerminationProtectedCluster/
