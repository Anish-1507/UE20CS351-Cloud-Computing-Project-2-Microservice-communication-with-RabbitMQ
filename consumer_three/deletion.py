import pika
import pymongo
# RabbitMQ connection setup


connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()
channel.queue_declare(queue='delete_record_queue')

# MongoDB connection setup
client = pymongo.MongoClient('mongodb://mongodb:27017/')
db = client['students_db']
collection = db['students_collection']

# Function to handle messages received from RabbitMQ
def delete_record_callback(ch, method, properties, body):
    srn = body.decode()
    result = collection.delete_one({'SRN': srn})
    if result.deleted_count == 1:
        print(f"Record with SRN {srn} deleted successfully")
    else:
        print(f"Unable to delete record with SRN {srn}")

# Start consuming messages from RabbitMQ
channel.basic_consume(queue='delete_record_queue', on_message_callback=delete_record_callback, auto_ack=True)
print('Delete record consumer is waiting for messages...')
channel.start_consuming()
