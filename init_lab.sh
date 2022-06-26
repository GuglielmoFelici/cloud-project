curl https://raw.githubusercontent.com/GuglielmoFelici/cloud-project/master/sandbox/lambdas/monitor.py > monitor.py
curl https://raw.githubusercontent.com/GuglielmoFelici/cloud-project/master/sandbox/lambdas/sensors.py > monitor.py

zip monitor.zip monitor.py
zip sensors.zip sensors.py

aws lambda create-function --function-name monitor --zip-file fileb://monitor.zip --handler monitor.lambda_handler --runtime python3.9 --role arn:aws:iam::225327119479:role/LabRole 
aws lambda create-function --function-name sensors --zip-file fileb://sensors.zip --handler sensors.lambda_handler --runtime python3.9 --role arn:aws:iam::225327119479:role/LabRole 

aws s3api create-bucket --bucket sandbox-guglielmo-data-lake --region us-east-1 
aws s3api create-bucket --bucket sandbox-spark-checkpoints --region us-east-1 


# FOR EC2


# python3 -m pip install jupyter
# python3 -m pip install ipyparallel
# python3 -m pip install pyspark --no-cache-dir

# port forward for jupyter
# ssh -i ~/.ssh/cloud-project.pem -N -f -L 8888:localhost:8888 ec2-user@ec2-52-90-4-225.compute-1.amazonaws.com (this one can be different, check in your shell or aws console)