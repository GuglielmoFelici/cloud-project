from string import printable
from time import time
import os
import json
import random
import boto3

s3 = boto3.client('s3')
client = boto3.client("s3")

FILENAME = "sensors.json"
BUCKET = "SANDBOX-guglielmo-data-lake"

SENSORS_NUMBER = 30  # can also use: int(os.environ['SENSORS_NUMBER'])

sensor_name = ["p", "t"]
area = ["industrial", "residential"]
customer = ["AB-Service", "Atlanta Group"]


def lambda_handler(event, context):
    device_info = []
    for _ in range(SENSORS_NUMBER):
        id = random.choice(
            sensor_name) + "-" + "".join(random.choice(printable[0:9]) for j in range(8))
        device_info.append(
            {
                "device_id": id,
                "device_health": random.randint(0, 10),
                "type": 'pollution' if id[0:1] == "p" else 'temperature',
                "area": random.choice(area),
                "customer": random.choice(customer)
            })
    with open(f'/tmp/{FILENAME}', 'w') as f:
        json.dump(device_info, f)
    client.upload_file(f'/tmp/{FILENAME}', BUCKET, f'monitor/{FILENAME}')

    return {
        'statusCode': 200
    }
