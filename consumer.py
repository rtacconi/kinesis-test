import boto3
import json
import uuid
import decimal
from botocore.exceptions import ClientError
import os
import datetime


def get_iterator(connection, stream_name, shard_id):
    return connection.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType='TRIM_HORIZON'
    )


def shards_info(connection, stream_name):
    '''
    Returns a dictionary with shard_id and iterator
    '''
    desc = connection.describe_stream(StreamName=stream_name)
    shard_ids = [d['ShardId'] for d in desc['StreamDescription']['Shards']]
    return [
        {
            'shard_id': shard_id,
            'iterator': get_iterator(connection, stream_name, shard_id)['ShardIterator']
        }
        for shard_id in shard_ids
    ]


def get_records(connection, shard, batch_size):
    response = connection.get_records(
        ShardIterator=shard['iterator'],
        Limit=batch_size
    )
    return {
        'records': [
            {
                'data': json.loads(r['Data'].decode()),
                'sequence_number': r['SequenceNumber'],
                'shard_id': shard['shard_id']
            }
            for r in response['Records']
        ],
        'iterator': response['NextShardIterator']
    }


def get_shard_row(shard_id):
    '''
    Find the row in the table of a shard ID.

    Parameters:
    shard_id (str): ID of the shard

    Returns:
    None: if the record does not exist
    dict: if the record exists it returns the row
    '''
    try:
        response = table.get_item(
            Key={'leaseKey': shard_id}
        )
        item = response['Item']
    except ClientError as e:
        print(e.response['Error']['Message'])
        return None
    except KeyError:
        return None
    else:
        return item


def status_code_update(response):
    '''
    Process the response from a reoced udpate.
    Parameters:
    response (dict): a response dictionary
    Returns:
    int: status code, 200 if successfull
    '''
    return response['ResponseMetadata']['HTTPStatusCode']


def update_checkpoint(records):
    '''Update the checkpoint with the sequence_number of the last record'''
    record = records[-1]
    row = get_shard_row(record['shard_id'])

    try:
        response = table.update_item(
            Key={
                'leaseKey': row['leaseKey']
            },
            UpdateExpression="set leaseCounter = :l, checkpoint = :c",
            ExpressionAttributeValues={
                ':l': decimal.Decimal(row['leaseCounter'] + 1),
                ':c': record['sequence_number']
            },
            ReturnValues="UPDATED_NEW"
        )
    except ClientError as e:
        print(e)
        return None
    else:
        return response


def create_checkpoint(records):
    r = records[-1]

    try:
        response = table.put_item(
           Item={
                'leaseKey': r['shard_id'],
                'checkpoint': r['sequence_number'],
                'leaseCounter': 0,
                'leaseOwner': worker_id
            }
        )
    except ClientError as e:
        print(e)
        return None
    else:
        return response


def event_time_to_s3_key(event, file_name):
    event_time_parsed = datetime.datetime.strptime(
        event,
        '%Y-%m-%d %H:%M:%S.%f'
    )
    return "y={}/m={}/d={}/h={}/{}.json".format(
        event_time_parsed.year,
        event_time_parsed.month,
        event_time_parsed.day,
        event_time_parsed.hour,
        file_name
    )


def process_records(s3_conn, bucket_name, records):
    if len(records) > 0:
        event_time = records[0]['data']['event_date_time']
        print(f"Processing, event time of batch is {event_time}.")
        data_records = [r['data'] for r in records]

        record_bytes = json.dumps(data_records)
        s3_conn.put_object(
            Body=record_bytes,
            Key='js/' + event_time_to_s3_key(event_time, str(uuid.uuid4())),
            Bucket=bucket_name
        )
        return True
    else:
        return False

# https://docs.aws.amazon.com/streams/latest/dev/kinesis-record-processor-ddb.html
# 1. leaseKey string - the shardID - Primary partition key
# 2. checkpoint string - the latest processed sequence number
# 3. leaseCounter integer - Used for lease versioning so that workers can
# detect that their lease has been taken by another worker.
# 4. leaseOwner string (uuid)


def main():
    os.environ['BUCKET_NAME'] = 'clickstream-riccardo-test'
    os.environ['BATCH_SIZE'] = '1000'
    batch_size = int(os.environ['BATCH_SIZE'])
    bucket_name = os.environ['BUCKET_NAME']
    shard_number = int(os.environ['SHARD_NUMBER'])
    session = boto3.session.Session(profile_name='aspire')
    worker_id = str(uuid.uuid4())
    dynamodb = session.resource(
        'dynamodb',
        region_name='eu-west-1',
        endpoint_url="https://dynamodb.eu-west-1.amazonaws.com"
    )
    stream_name = 'clickstream_anto_tealium_page_view'
    # it should load or create a table using stream_name as table name
    table = dynamodb.Table('PythonKCLSample')  # TODO: must be changed
    connection = session.client('kinesis')
    s3_conn = session.client('s3')
    shards = shards_info(connection, stream_name)
    shard = shards[shard_number]
    response = get_shard_row(shard['shard_id'])

    try:
        while True:
            try:
                records = get_records(connection, shard, batch_size)
            except ClientError:
                print("Client error")
            else:
                print("Fetched {} records, shard ID: {}.".format(
                        len(records['records']), shard['shard_id']
                    )
                )
                sequence_numbers = [
                    r['sequence_number'] for r in records['records']]
                response = get_shard_row(shard['shard_id'])

                if response is None:
                    # it could not find any breakpoint
                    process_records(s3_conn, bucket_name, records['records'])
                    create_checkpoint(records['records'])
                else:
                    if response['checkpoint'] in sequence_numbers:
                        # found a seq. number in the records that is our checkpoint
                        print('duplicate')
                    else:
                        process_records(s3_conn, bucket_name, records['records'])
                        code = status_code_update(update_checkpoint(records['records']))
                        print(f"Checkpoint creation return code: {code}")

                # update the next iterator
                shard = {
                    'shard_id': shard['shard_id'],
                    'iterator': records['iterator']
                }
    except KeyboardInterrupt:
        print('SIGINT received, stopping!')

if __name__== "__main__":
  main()
  
# create_table('kinesis')
# def create_table(name):
#     table = dynamodb.create_table(
#         TableName=name,
#         KeySchema=[
#             {
#                 "AttributeName": "leaseKey",
#                 "KeyType": "HASH"
#             }
#         ],
#         AttributeDefinitions=[
#             {
#                 "AttributeName": "leaseKey",
#                 "AttributeType": "S"
#             }
#         ],
#         ProvisionedThroughput={
#             'ReadCapacityUnits': 10,
#             'WriteCapacityUnits': 10
#         }
#     )
#
#     return "Table status:".format(table.table_status)
