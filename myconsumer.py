from kafka import KafkaConsumer
from json import loads
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('customer_details')

consumer = KafkaConsumer(
    'sales-events',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))
    
price = {'total_sales': 0.0}
product_sales = dict()
product_sales_amount = dict()
     
for message in consumer:
    message = message.value
    #print(message)
    customer_id = message.get('user_id', 'null')
    message['customer_id'] = customer_id
    if 'customer_id' in message and 'event_type' in message:
        table.put_item(Item=message)
        
    event_type = message.get('event_type', 'null_event_type')
    item_price = message.get('price', '0.0')
    item_price = float(item_price)
    brand = message.get('brand', 'unknown_brand')
    if event_type == 'purchase':
    	price['total_sales'] += item_price
    	if brand in product_sales:
    		product_sales[brand] += 1
    	else:
    		product_sales[brand] = 1
    		
    	if brand in product_sales_amount:
    		product_sales_amount[brand] += item_price
    	else:
    		product_sales_amount[brand] = item_price
    		
    print(price)
    print(product_sales)
    print(product_sales_amount)



