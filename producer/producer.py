from flask import Flask, request
import pika


app = Flask(__name__)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

# Create the exchanges and queues
channel.exchange_declare(exchange='health_check_exchange', exchange_type='direct')
channel.exchange_declare(exchange='insert_record_exchange', exchange_type='direct')
channel.exchange_declare(exchange='read_database_exchange', exchange_type='direct')
channel.exchange_declare(exchange='delete_record_exchange', exchange_type='direct')

channel.queue_declare(queue='health_check_queue')
channel.queue_declare(queue='insert_record_queue')
channel.queue_declare(queue='read_database_queue')
channel.queue_declare(queue='delete_record_queue')

channel.queue_bind(exchange='health_check_exchange', queue='health_check_queue', routing_key='health_check')
channel.queue_bind(exchange='insert_record_exchange', queue='insert_record_queue', routing_key='insert_record')
channel.queue_bind(exchange='read_database_exchange', queue='read_database_queue', routing_key='read_database')
channel.queue_bind(exchange='delete_record_exchange', queue='delete_record_queue', routing_key='delete_record')

# Function to publish messages to RabbitMQ
def publish_message(exchange, routing_key, message):
    channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message)

# Health check endpoint
@app.route('/health_check', methods=['GET'])
def health_check():
    message = request.args.get('message')
    publish_message('health_check_exchange', 'health_check', message)
    return 'Health check request sent to RabbitMQ'

# Insert record endpoint
@app.route('/insert_record', methods=['POST'])
def insert_record():
    data = request.json
    message = f"{data['Name']},{data['SRN']},{data['Section']}"
    publish_message('insert_record_exchange', 'insert_record', message)
    return 'Insert record request sent to RabbitMQ'

# Read database endpoint
@app.route('/read_database', methods=['GET'])
def read_database():
    publish_message('read_database_exchange', 'read_database', '')
    return 'Read database request sent to RabbitMQ'

# Delete record endpoint
@app.route('/delete_record', methods=['GET'])
def delete_record():
    srn = request.args.get('SRN')
    publish_message('delete_record_exchange', 'delete_record', srn)
    return f"Delete record request sent to RabbitMQ for SRN: {srn}"
if __name__ == '__main__':
    app.run(port=5050, host="0.0.0.0")
''