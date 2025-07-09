!#/bin/bash

# Set the version variables
export AIRFLOW_VERSION=3.0.2
export PYTHON_VERSION=3.12

# Download the matching constraints file
curl -sSL "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" -o constraints.txt

# Install using constraints
pip install -r requirements.txt -c constraints.txt
