import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError, \
    MessageMiddlewareMessageError, MessageMiddlewareCloseError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError from e

        try:
            channel = connection.channel()
            channel.queue_declare(queue=queue_name)
            channel.basic_qos(prefetch_count=1)
        except pika.exceptions.AMQPError as e:
            if connection.is_open:
                connection.close()
            raise MessageMiddlewareMessageError from e

        self.channel = channel
        self.connection = connection
        self.queue_name = queue_name
        self.consumer_tag = None

    def start_consuming(self, on_message_callback) -> None:
        def callback(ch, method, _properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        try:
            self.consumer_tag = self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
            self.channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError from e

        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError from e

        finally:
            self.consumer_tag = None

    def stop_consuming(self) -> None:
        if not self.consumer_tag:
            return
        try:
            self.channel.stop_consuming(self.consumer_tag)
            self.consumer_tag = None
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError from e

    def send(self, message) -> None:
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)

        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError

        except pika.exceptions.AMQPError:
            raise MessageMiddlewareMessageError

    def close(self) -> None:
        try:
            if self.connection.is_open:
                # Cerrar la conexion cierra todos los canales abiertos
                self.connection.close()
        except pika.exceptions.AMQPError:
            raise MessageMiddlewareCloseError


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError from e

        try:
            channel = connection.channel()
            channel.exchange_declare(exchange=exchange_name, exchange_type="topic")
        except pika.exceptions.AMQPError as e:
            if connection.is_open:
                connection.close()
            raise MessageMiddlewareMessageError from e

        self.channel = channel
        self.connection = connection
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.consumer_tag = None

    def start_consuming(self, on_message_callback) -> None:
        def callback(ch, method, _properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        try:
            result = self.channel.queue_declare(queue='', exclusive=True)
            for key in self.routing_keys:
                self.channel.queue_bind(exchange=self.exchange_name, queue=result.method.queue, routing_key=key)

            self.consumer_tag = self.channel.basic_consume(queue=result.method.queue, on_message_callback=callback)
            self.channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError from e

        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError from e

        finally:
            self.consumer_tag = None

    def stop_consuming(self) -> None:
        if not self.consumer_tag:
            return
        try:
            self.channel.stop_consuming(self.consumer_tag)
            self.consumer_tag = None
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError from e

    def send(self, message) -> None:
        try:
            for key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name, routing_key=key, body=message)

        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError

        except pika.exceptions.AMQPError:
            raise MessageMiddlewareMessageError

    def close(self) -> None:
        try:
            if self.connection.is_open:
                # Cerrar la conexion cierra todos los canales abiertos
                self.connection.close()
        except pika.exceptions.AMQPError:
            raise MessageMiddlewareCloseError

