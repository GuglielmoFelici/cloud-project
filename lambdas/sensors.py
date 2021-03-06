import datetime
import json
import random
import boto3

s3 = boto3.client('s3')
client = boto3.client("s3")

FILENAME_PREFIX = 'measurament'
BUCKET = 'guglielmo-data-lake'
SENSORS_DATA_KEY = 'all_sensors.json'


def lambda_handler(event, context):

    response = s3.get_object(Bucket=BUCKET, Key=SENSORS_DATA_KEY)['Body']
    sensors = json.load(response)

    ''' choose random device '''
    device = random.choice(sensors)
    device_id = device['device_id']

    ''' generate random data according to device type '''
    now = datetime.datetime.now()
    data = {
        "device_id": device_id,
        "timestamp": now.isoformat(),
        "arrived": (now + datetime.timedelta(days=random.randint(0, 1))).isoformat()
    }
    if device["type"] == "temperature":
        data["humidity"] = random.randint(0, 100)
        data["temperature"] = random.randint(-5, 35)
    elif device["type"] == "pollution":
        data["CO2_level"] = random.randint(300, 450)  # ppm

    ''' write output on S3 '''
    filename = f'{device_id}_{now}.json'
    with open(f'/tmp/{filename}', 'w') as f:
        json.dump(data, f)
    client.upload_file(f'/tmp/{filename}',
                       BUCKET, f'measurements/{device["type"]}/{filename}')

    return {
        'statusCode': 200
    }
