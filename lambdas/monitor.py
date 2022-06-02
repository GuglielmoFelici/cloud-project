from string import printable
from time import time
import os
import json
import random
import boto3

s3 = boto3.client('s3')
client = boto3.client("s3")

FILENAME = "sensors.json"
BUCKET = "myjsonbucketest"

sensor_name = ["p","t"]
area = ["industrial", "residential"]
customer = ["AB-Service", "Atlanta Group"]

class DeviceInfo:
    def __init__(self, device_id, device_healt, type, area, customer):
        self.device_id = device_id
        self.device_health = device_healt
        self.type = type
        self.area = area
        self.customer = customer

    def update_value(cls, device_health):
        cls.device_health = device_health

def createSensor():

    SENSORS_NUMBER = int(os.environ['SENSORS_NUMBER'])
    device_info = []
    
    i = 0
    f = open(f'/tmp/{FILENAME}','a')
    
    while i < SENSORS_NUMBER:
       id = random.choice(sensor_name) + "-" + "".join(random.choice(printable[0:9]) for j in range(8))
       d = DeviceInfo(id, random.randint(0,10), 'pollution' if id[0:1] == "p" else 'temperature', random.choice(area), random.choice(customer))
       device_info.append(d)
        
       f.write(json.dumps(d.__dict__)+'\n')
        
       client.upload_file(f'/tmp/{FILENAME}', BUCKET, f'json/{FILENAME}')
       i += 1
     
    f.close()     
    
def lambda_handler(event, context):
    createSensor()
    return {
    'statusCode': 200,
    'body': json.dumps("ok")
    }
