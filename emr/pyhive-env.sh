#!/bin/bash

wget https://repo.continuum.io/archive/Anaconda3-2020.07-Linux-x86_64.sh

bash Anaconda3-2020.07-Linux-x86_64.sh



pip install pandas

pip install pyhive

pip install wheel

pip install thrift

sudo yum install python3-devel

sudo yum install gcc-c++ python-devel.x86_64 cyrus-sasl-devel.x86_64

pip install thrift-sasl
