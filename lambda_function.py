import json
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal
from boto3.dynamodb.conditions import Key

# Initialize the DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
leader_info_table = dynamodb.Table('leader-info')
satsang_count_table = dynamodb.Table('satsang-count')
upcoming_events_table = dynamodb.Table('upcoming-events')
kids_table = dynamodb.Table('kids-list')

leader_info = '/leaderInfo'
satsang_count = '/satsangCount'
upcoming_events = '/upcomingEvents'
kids = '/kids'

def lambda_handler(event, context):
    print('Request event: ', event)
   
    try:
        http_method = event.get('httpMethod')
        path = event.get('path')

        if http_method == 'GET' and path == leader_info:
            mandir_name = event['queryStringParameters']['mandirName']
            return get_leader_info(mandir_name)
        elif http_method == 'POST' and path == leader_info:
            return post_leader_info(json.loads(event['body'])) 
        elif http_method == 'GET' and path == satsang_count:
            return get_satsang_count()
        elif http_method == 'POST' and path == satsang_count:
            return post_satsang_count(json.loads(event['body']))
        elif http_method == 'GET' and path == upcoming_events:
            return get_upcoming_events()
        elif http_method == 'POST' and path == upcoming_events:
            return post_upcoming_events(json.loads(event['body']))
        elif http_method == 'GET' and path == kids:
            return get_kids()
        elif http_method == 'POST' and path == kids:
            return post_kid(json.loads(event['body']))
        else:
            return build_response(404, '404 Not Found')

    except Exception as e:
        print('Error:', e)
        return build_response(400, 'Error processing request')

def get_leader_info(mandir_name):
    try:
        response = leader_info_table.get_item(Key={'mandirName': mandir_name})
        return build_response(200, response.get('Item'))
    except ClientError as e:
        print('Error:', e)
        return build_response(400, e.response['Error']['Message'])

def post_leader_info(request_body):
    try:
        leader_info_table.put_item(Item=request_body)
        body = {
            'Operation': 'SAVE',
            'Message': 'SUCCESS',
            'Item': request_body
        }
        return build_response(200, body)
    except ClientError as e:
        print('Error:', e)
        return build_response(400, e.response['Error']['Message'])

def get_satsang_count():
    try:
        scan_params = {
            'TableName': satsang_count_table.name
        }
        return build_response(200, scan_dynamo_records_satsang_counts(scan_params, []))
    except ClientError as e:
        print('Error:', e)
        return build_response(400, e.response['Error']['Message'])

def scan_dynamo_records_satsang_counts(scan_params, item_array):
    response = satsang_count_table.scan(**scan_params)
    item_array.extend(response.get('Items', []))
   
    if 'LastEvaluatedKey' in response:
        scan_params['ExclusiveStartKey'] = response['LastEvaluatedKey']
        return scan_dynamo_records(scan_params, item_array)
    else:
        return {'satsang_count': item_array}

def post_satsang_count(request_body):
    try:
        satsang_count_table.put_item(Item=request_body)
        body = {
            'Operation': 'SAVE',
            'Message': 'SUCCESS',
            'Item': request_body
        }
        return build_response(200, body)
    except ClientError as e:
        print('Error:', e)
        return build_response(400, e.response['Error']['Message'])
    
def get_upcoming_events():
    try:
        scan_params = {
            'TableName': upcoming_events_table.name
        }
        return build_response(200, scan_dynamo_records_events(scan_params, []))
    except ClientError as e:
        print('Error:', e)
        return build_response(400, e.response['Error']['Message'])


def post_upcoming_events(request_body):
    try:
        upcoming_events_table.put_item(Item=request_body)
        body = {
            'Operation': 'SAVE',
            'Message': 'SUCCESS',
            'Item': request_body
        }
        return build_response(200, body)
    except ClientError as e:
        print('Error:', e)
        return build_response(400, e.response['Error']['Message'])
        
def get_kids():
    try:
        scan_params = {
            'TableName': kids_table.name
        }
        return build_response(200, scan_dynamo_records(scan_params, []))
    except ClientError as e:
        print('Error:', e)
        return build_response(400, e.response['Error']['Message'])

def scan_dynamo_records(scan_params, item_array):
    response = kids_table.scan(**scan_params)
    item_array.extend(response.get('Items', []))
   
    if 'LastEvaluatedKey' in response:
        scan_params['ExclusiveStartKey'] = response['LastEvaluatedKey']
        return scan_dynamo_records(scan_params, item_array)
    else:
        return {'kids': item_array}
        
def scan_dynamo_records_events(scan_params, item_array):
    response = upcoming_events_table.scan(**scan_params)
    item_array.extend(response.get('Items', []))
   
    if 'LastEvaluatedKey' in response:
        scan_params['ExclusiveStartKey'] = response['LastEvaluatedKey']
        return scan_dynamo_records(scan_params, item_array)
    else:
        return {'upcomingEvents': item_array}

def post_kid(request_body):
    try:
        kids_table.put_item(Item=request_body)
        body = {
            'Operation': 'SAVE',
            'Message': 'SUCCESS',
            'Item': request_body
        }
        return build_response(200, body)
    except ClientError as e:
        print('Error:', e)
        return build_response(400, e.response['Error']['Message'])

def build_response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            "Access-Control-Allow-Origin" : "*",
            "Access-Control-Allow-Credentials" : 'true',
        },
        'body': json.dumps(body, cls=DecimalEncoder)
    }

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            # Check if it's an int or a float
            if obj % 1 == 0:
                return int(obj)
            else:
                return float(obj)
        # Let the base class default method raise the TypeError
        return super(DecimalEncoder, self).default(obj)