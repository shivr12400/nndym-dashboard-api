import json
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal
from boto3.dynamodb.conditions import Key

# Initialize the DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
leader_info_table = dynamodb.Table('leader-info')
satsang_count_table = dynamodb.Table('satsang-count')
upcoming_events_table = dynamodb.Table('upcoming_events')

leader_info = '/leaderInfo'
satsang_count = '/satsangCount'
upcoming_events = '/upcomingEvents'

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
            mandir_name = event['queryStringParameters']['mandirName']
            date = event['queryStringParameters']['date']
            return get_satsang_count(mandir_name, date)
        elif http_method == 'POST' and path == satsang_count:
            return post_satsang_count(json.loads(event['body']))
        elif http_method == 'GET' and path == upcoming_events:
            mandir_name = event['queryStringParameters']['mandirName']
            return get_upcoming_events(mandir_name)
        elif http_method == 'POST' and path == upcoming_events:
            return post_upcoming_events(json.loads(event['body']))
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

def get_satsang_count(mandir_name, date):
    try:
        response = satsang_count_table.get_item(Key={'mandirName': mandir_name, "date": date})
        return build_response(200, response.get('Item'))
    except ClientError as e:
        print('Error:', e)
        return build_response(400, e.response['Error']['Message'])

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
    
def get_upcoming_events(mandir_name):
    try:
        response = upcoming_events_table.get_item(Key={'mandirName': mandir_name})
        return build_response(200, response.get('Item'))
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

def build_response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json'
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