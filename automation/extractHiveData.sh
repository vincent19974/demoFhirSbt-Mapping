#!/bin/bash
mkdir -p ~/miniconda3
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm -rf ~/miniconda3/miniconda.sh
~/miniconda3/bin/conda init bash
~/miniconda3/bin/conda init zsh
eval "$(/home/hadoop/miniconda3/bin/conda shell.bash hook)" \
	&& conda create --name py38 python=3.8 -y \
	&& conda init zsh \
	&& eval "$(conda shell.bash hook)" \
        && conda activate py38
		
pip3 install pandas
pip3 install pyhive
pip3 install wheel
pip3 install thrift
sudo yum install python3-devel -y
sudo yum install python-pip gcc gcc-c++ python-virtualenv cyrus-sasl-devel -y
pip3 install thrift-sasl
whoami

mkdir -p /tmp/test/ 
sudo chmod 777 -R /tmp/test/ 
rm -rf /tmp/test/extractHiveData.py 
rm -rf /tmp/test/result.csv 
aws s3 cp s3://aws-logs-723293022411-us-east-1/bootstrap-test/extractHiveData.py /tmp/test/ 
echo "Downloaded Python Script from S3." 
python3 /tmp/test/extractHiveData.py  
echo "Python Script executed."

