#!/bin/bash
source venv/bin/activate

export AIRFLOW_HOME=airflow
export PROJECT_BASE=$(pwd)
export PYTHONPATH=$PYTHONPATH:$(pwd)

# The RSA permission to use for AWS ssh/scp
export AWS_PERMISSION_FILE_URL=~/Documents/aws/aws.pem

export export AIRFLOW__CORE__FERNET_KEY=$(cat fernet.key)


airflow scheduler &
airflow webserver -p 8080 &

deactivate