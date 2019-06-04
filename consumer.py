import boto3
import json
import uuid
import decimal
from botocore.exceptions import ClientError
import os
import datetime
from time import sleep


def create_table(client, name):
    '''
    Create a dynamodb table
    Paramters:
    client (botocore.client.DynamoDB): the DynamoDB client
    name (str): name of the table
    Returns:
    string: the state of the table, i.e. 'ACTIVE'
    '''
    table = client.create_table(
        TableName=name,
        KeySchema=[
            {
                "AttributeName": "leaseKey",
                "KeyType": "HASH"
            }
        ],
        AttributeDefinitions=[
            {
                "AttributeName": "leaseKey",
                "AttributeType": "S"
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )

    return table['TableDescription']['TableStatus']


def table_exits(client, name):
    '''
    Check if the dynamodb table exists
    Paramters:
    client (botocore.client.DynamoDB): the DynamoDB client
    name (str): name of the table
    Returns:
    bool: True if it exists, False if not, None on error
    '''
    try:
        client.describe_table(TableName=name)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            return False
        else:
            return None


def get_table_status(client, table_name):
    '''
    Get the status of a dynamodb table
    Paramters:
    client (botocore.client.DynamoDB): the DynamoDB client
    table_name (str): name of the table
    Returns:
    string: the state of the table, i.e. 'ACTIVE'
    '''
    return client.describe_table(
        TableName='PythonKCLSamplew'
    )['Table']['TableStatus']


def table_esists_or_create(client, table_name):
    if not table_exits(client, table_name):
        print(f"Table created, status: {create_table(client, table_name)}")
        status = get_table_status(client, table_name)
    else:
        status = ''

    while not status == 'ACTIVE':
        # poll until the status will be ACTIVE
        status = get_table_status(client, table_name)
        sleep(1)

    print(f"Table status: {status}")
    return status


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


def get_shard_row(table, shard_id):
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


def update_checkpoint(table, worker_id, records):
    '''
    Update the checkpoint with the sequence_number of the last record
    Paramters:
    table: DynamoDB table to query
    worker_id (str(uuid)): the worker ID
    records (dict): the records received from kinesis
    Returns:
    response (None|dict): the response with the HTTP result
    '''
    record = records[-1]
    row = get_shard_row(table, record['shard_id'])

    try:
        response = table.update_item(
            Key={
                'leaseKey': row['leaseKey']
            },
            UpdateExpression="set leaseCounter = :l, checkpoint = :c, leaseOwner = :o",
            ExpressionAttributeValues={
                ':l': decimal.Decimal(row['leaseCounter'] + 1),
                ':c': record['sequence_number'],
                ':o': worker_id
            },
            ReturnValues="UPDATED_NEW"
        )
    except ClientError as e:
        print(e)
        return None
    else:
        return response


def create_checkpoint(table, worker_id, records):
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


def main():
    # the following ENV variables will have to go
    os.environ['BUCKET_NAME'] = 'clickstream-riccardo-test'
    os.environ['BATCH_SIZE'] = '1000'
    os.environ['PREFIX'] = 'clickstream'
    prefix = os.environ['PREFIX']
    batch_size = int(os.environ['BATCH_SIZE'])
    bucket_name = os.environ['BUCKET_NAME']
    shard_number = int(os.environ['SHARD_NUMBER'])
    region_name = 'eu-west-1'
    session = boto3.session.Session(profile_name='aspire')
    client_dynamo = session.client('dynamodb', region_name)
    stream_name = 'clickstream_anto_tealium_page_view'
    table_name = prefix + "_" + stream_name
    table_esists_or_create(client_dynamo, table_name)
    worker_id = str(uuid.uuid4())
    dynamodb = session.resource(
        'dynamodb',
        region_name=region_name,
        endpoint_url=f"https://dynamodb.{region_name}.amazonaws.com"
    )
    # it should load or create a table using stream_name as table name
    table = dynamodb.Table('PythonKCLSample')  # TODO: must be changed
    connection = session.client('kinesis')
    s3_conn = session.client('s3')
    shards = shards_info(connection, stream_name)
    shard = shards[shard_number]
    response = get_shard_row(table, shard['shard_id'])

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
                response = get_shard_row(table, shard['shard_id'])

                if response is None:
                    # it could not find any breakpoint
                    process_records(s3_conn, bucket_name, records['records'])
                    create_checkpoint(table, worker_id, records['records'])
                else:
                    if response['checkpoint'] in sequence_numbers:
                        # found a seq. number in the records that is our checkpoint
                        print('duplicate')
                    else:
                        process_records(s3_conn, bucket_name, records['records'])
                        code = status_code_update(
                            update_checkpoint(table, worker_id, records['records']))
                        print(f"Checkpoint creation return code: {code}")

                # update the next iterator
                shard = {
                    'shard_id': shard['shard_id'],
                    'iterator': records['iterator']
                }
    except KeyboardInterrupt:
        print('SIGINT received, stopping!')


if __name__ == "__main__":
    main()
