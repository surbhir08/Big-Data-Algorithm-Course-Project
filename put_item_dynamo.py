import boto3

client = boto3.client('dynamodb')


response = client.put_item(
    Item={
        'customer_id': {
            'S': 'C1',
        },
        'event_type': {
            'S': 'purchase',
        }
    },
    ReturnConsumedCapacity='TOTAL',
    TableName='customer_details',
)

print(response)
