# tpcds-lithops-scripts

This application generates data and executes TPCDS queries using the cloud. It uses Lithops framework which makes it easy to deploy to different clouds such as IBM, Google Cloud, Azure and AWS. 
However, it has only been tested on AWS using AWS Lambda for processing and S3 for storage.

# Build 

Build the included Dockerfile and upload to the appropriate cloud using the instructions at https://github.com/lithops-cloud/lithops/tree/master/runtime.

For AWS Lambda: lithops runtime build -f MyDockerfile docker_username/my_container_runtime -b aws_lambda.

Add config file ```.lithops_config```

# Run

To generate data: ```python tpcds-gen.py```.

To execute queries use one of the 4 scripts - tpcds-1.py, tpcds-16.py, tpcds-94.py, tpcds-95.py. E.g. ```python tpcds-1.py```

