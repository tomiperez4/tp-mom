import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, \
    MessageMiddlewareMessageError


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError

        channel = connection.channel()
        try:
            channel.queue_declare(queue=queue_name)
            channel.basic_qos(prefetch_count=1)
        except pika.exceptions.AMQPError:
            connection.close()
            raise MessageMiddlewareMessageError

        self.channel = channel
        self.connection = connection
        self.queue_name = queue_name

    def start_consuming(self, on_message_callback):
        def callback(ch, method, _properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        try:
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
        except pika.exceptions.AMQPError:
            raise MessageMiddlewareMessageError
        except Exception as e:
            raise f'Unknown error: {e}'

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
