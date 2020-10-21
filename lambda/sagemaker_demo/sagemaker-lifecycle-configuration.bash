#!/bin/bash

set -e

# OVERVIEW
# This script executes an existing Notebook file on the instance during start using nbconvert(https://github.com/jupyter/nbconvert)

# PARAMETERS

ENVIRONMENT=python3
NOTEBOOK_FILE=/home/ec2-user/SageMaker/Demo/read-write-pandas.ipynb

source /home/ec2-user/anaconda3/bin/activate "$ENVIRONMENT"

jupyter nbconvert "$NOTEBOOK_FILE" --ExecutePreprocessor.kernel_name=conda_python3 --execute

source /home/ec2-user/anaconda3/bin/deactivate
