import boto3

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('measurements')

def lambda_handler(event, context):
    table.put_item(
        Item={'sensor_data':'22022-06-01 00:00:00', 'device_id': '1', 'measured': '2022-06-01 00:00:00', 'arrived': '2022-06-01 00:00:00', 'pollution': 6, 'temperature': 6, 'device_health': 0, 'type': 'temperature', 'area': 'residential', 'customer': 'AB-Service'}
    )
