import pika
import json
from pymongo import MongoClient


connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()
# Create the exchange and queue
channel.exchange_declare(exchange='insert_record_exchange', exchange_type='direct')
channel.queue_declare(queue='insert_record_queue')
channel.queue_bind(exchange='insert_record_exchange', queue='insert_record_queue', routing_key='insert_record')

# MongoDB client setup
client = MongoClient('mongodb://mongodb:27017/')
db = client['students_db']
collection = db['students_collection']

# Function to handle incoming messages from the insert_record queue
def insert_record_callback(ch, method, properties, body):
    data = body.decode('utf-8').split(',')
    name, srn, section = data[0], data[1], data[2]
    record = {'Name': name, 'SRN': srn, 'Section': section}
    collection.insert_one(record)
    print(f"Record inserted into database: {record}")

# Consume messages from the insert_record queue
channel.basic_consume(queue='insert_record_queue', on_message_callback=insert_record_callback, auto_ack=True)

print('Insert record consumer is waiting for messages...')
channel.start_consuming()
