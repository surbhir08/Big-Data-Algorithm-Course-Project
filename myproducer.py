from kafka import KafkaProducer
from json import dumps
import csv
from time import sleep


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
  
with open('purchase_data.csv', 'r') as fp:
	for lines in csv.DictReader(fp):
		print(lines)
		#sleep(2)
		producer.send('sales-events', value=lines)
  
                        

    
