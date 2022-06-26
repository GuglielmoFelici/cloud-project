'''
USE THIS LAMBDA ONLY ONCE, at the beginning of the simulation, to generate the set of ALL possible sensors.
'''

from string import printable
from time import time
import os
import json
import random
import boto3

s3 = boto3.client('s3')
client = boto3.client("s3")

FILENAME = "all_sensors.json"
BUCKET = "guglielmo-data-lake"

SENSORS_NUMBER = 100  # can also use: int(os.environ['SENSORS_NUMBER'])

sensor_name = ["p", "t"]


def lambda_handler(event, context):
    device_info = []
    for _ in range(SENSORS_NUMBER):
        id = random.choice(
            sensor_name) + "-" + "".join(random.choice(printable[0:9]) for _ in range(8))
        device_info.append(
            {
                "device_id": id,
                "type": 'pollution' if id[0:1] == "p" else 'temperature',
            })
    with open(f'/tmp/{FILENAME}', 'w') as f:
        json.dump(device_info, f)
    client.upload_file(f'/tmp/{FILENAME}', BUCKET, f'{FILENAME}')

    return {
        'statusCode': 200
    }
