from string import printable
from time import time
import os
import json
import random
import boto3

s3 = boto3.client('s3')
client = boto3.client("s3")

FILENAME = "sensors.json"
BUCKET = "guglielmo-data-lake"
SENSORS_DATA_KEY = 'all_sensors.json'


area = ["industrial", "residential"]
customer = ["AB-Service", "Atlanta Group"]


def lambda_handler(event, context):

    response = s3.get_object(Bucket=BUCKET, Key=SENSORS_DATA_KEY)['Body']
    sensors = json.load(response)

    device_info = []
    for sensor in sensors:
        sensor['device_health'] = random.randint(0, 10)
        sensor['area'] = random.choice(area)
        sensor['customer'] = random.choice(customer)
        device_info.append(sensor)
    with open(f'/tmp/{FILENAME}', 'w') as f:
        json.dump(device_info, f)
    client.upload_file(f'/tmp/{FILENAME}', BUCKET, f'monitor/{FILENAME}')

    return {
        'statusCode': 200
    }
