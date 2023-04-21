import pika
import pymongo

# Connect to MongoDB database
client = pymongo.MongoClient('mongodb://mongodb:27017/')
db = client['students_db']
collection = db['students_collection']

connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

# Create the exchanges and queues
channel.exchange_declare(exchange='read_database_exchange', exchange_type='direct')
channel.queue_declare(queue='read_database_queue')
channel.queue_bind(exchange='read_database_exchange', queue='read_database_queue', routing_key='read_database')

def callback(ch, method, properties, body):
    # Retrieve all records from database
    records = collection.find()
    
    # Print records to console
    for record in records:
        print(record)
        
    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Consume messages from RabbitMQ
channel.basic_consume(queue='read_database_queue', on_message_callback=callback)
print('Read record consumer is waiting for messages...')
# Start consuming messages
channel.start_consuming()
