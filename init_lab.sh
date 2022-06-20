curl https://raw.githubusercontent.com/GuglielmoFelici/cloud-project/master/lambdas/monitor.py > monitor.py
curl https://raw.githubusercontent.com/GuglielmoFelici/cloud-project/master/lambdas/sensors.py > monitor.py

zip monitor.zip monitor.py
zip sensors.zip sensors.py

aws lambda create-function --function-name monitor --zip-file fileb://monitor.zip --handler monitor.lambda_handler --runtime python3.9 --role arn:aws:iam::225327119479:role/LabRole 