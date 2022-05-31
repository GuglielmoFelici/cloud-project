from time import time
from datetime import date
import json
import random
import boto3

s3 = boto3.client('s3')
client = boto3.client("s3")

FILENAME_PREFIX = 'measurament'
BUCKET = 'test-guglielmo-1'
SENSORS_DATA_KEY = 'json/sensors.json'


def lambda_handler(event, context):

    today = date.today()
    response = s3.get_object(Bucket=BUCKET, Key=SENSORS_DATA_KEY)['Body']

    ''' choose random device '''
    sensors = list(response.iter_lines())
    device = json.loads(random.choice(sensors))
    device_id = device['device_id']

    ''' generate random data according to device type '''
    data = {
        "device_id": device_id,
        "timestamp": time(),
    }
    if device["type"] == "temperature":
        data["humidity"] = random.randint(0, 100),
        data["temperature"] = random.randint(-5, 35),
    elif device["type"] == "pollution":
        data["CO2_level"] = random.randint(300, 450),  # ppm

    ''' write output on S3 '''
    dirname = today.strftime("%b-%d-%Y")
    filename = f'{FILENAME_PREFIX}-{str(time())}-{device_id}'
    with open(f'/tmp/{filename}', 'a') as f:
        f.write(json.dumps(data))
    client.upload_file(f'/tmp/{filename}',
                       'myjsonbucketest', f'json/{dirname}/{filename}')

    return {
        'statusCode': 200,
        'body': json.dumps(data)
    }
