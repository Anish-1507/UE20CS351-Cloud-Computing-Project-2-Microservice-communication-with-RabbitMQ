import pika


connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

# Declare the health_check queue
channel.queue_declare(queue='health_check_queue')

# Define the callback function to process incoming messages
def callback(ch, method, properties, body):
    print("Received health-check message: %r" % body)
    # Perform the health check here
    # ...
    # Acknowledge that the message has been processed
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Set up the consumer to listen for incoming messages
channel.basic_consume(queue='health_check_queue', on_message_callback=callback)

# Start consuming messages
print("Waiting for health-check messages...")
channel.start_consuming()