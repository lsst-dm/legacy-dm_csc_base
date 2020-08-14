# This file is part of dm_csc_base
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
import logging
import pika
from lsst.dm.csc.base.YamlHandler import YamlHandler
from pika.adapters.asyncio_connection import AsyncioConnection

LOGGER = logging.getLogger(__name__)


class Consumer(object):
    """Consumer handles RabbitMQ incoming messages


    Parameters
    ----------
    amqp_url : `str`
    csc_parent : `lsst.dm.csc.base.dm_csc`
    queue : `str`
    callback : `method`
    """

    def __init__(self, amqp_url, csc_parent, queue, callback):
        # only emit logging messages from pika and WARNING and above
        logging.getLogger("pika").setLevel(logging.WARNING)
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._publisher = None

        self._url = amqp_url
        self.csc_parent = csc_parent
        self.QUEUE = queue
        self.ROUTING_KEY = queue

        self._yaml_handler = YamlHandler(callback)
        self._message_callback = self._yaml_handler.yaml_callback

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.


        Returns
        -------
        AsyncioConnection

        """
        LOGGER.info('Connecting...')
        return AsyncioConnection(parameters=pika.URLParameters(self._url),
                                 on_open_callback=self.on_connection_open,
                                 on_open_error_callback=self.on_connection_open_error)

    def on_connection_open(self, _unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it.

        Parameters
        ----------
        unused_connection : `pika.Connection`
            unused - required by the API

        """
        LOGGER.info(f'Connection opened for {self.QUEUE}')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.

        Parameters
        ----------
        _unused_connection : `AsyncioConnection`
            unused - required by the API

        err :   Exception error
            The error that occurred
        """
        LOGGER.error(f'Connection open failed: {err}')
        if self.csc_parent is not None:
            self.csc_parent.fault(5071, 'failed to open connection to broker')

    def add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.
        """
        LOGGER.info('Adding connection close callback')

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.
        """
        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        Parameters
        ----------
        channel : pika.channel.Channel
            The channel object
        """
        LOGGER.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.
        """
        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        Parameters
        ----------
        channel : Channel
            The closed channel
        reason : `str`
            The reason the channel was closed
        """
        LOGGER.warning('Channel %i was closed: %s' % (channel, reason))
        self._connection.close()

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        Parameters
        ----------
        exchange_name : `str`
            The name of the exchange to declare
        """

        self._channel.exchange_declare(exchange='message', exchange_type='direct', durable=True,
                                       callback=self.on_exchange_declareok)

    def on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        Parameters
        ----------
        unused_frame: `pika.Frame.Method`
            Exchange.DeclareOk response frame
        """
        LOGGER.info('Exchange declared')
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        Parameters
        ----------
        queue_name : `str`
            The name of the queue to declare.
        """
        LOGGER.info('Declaring queue %s', queue_name)
        self._channel.queue_declare(callback=self.on_queue_declareok, queue=queue_name, durable=True)

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        Parameters
        ----------
        method_frame: `pika.frame.Method`
            The Queue.DeclareOk frame
        """
        LOGGER.info('Binding %s to %s with %s',
                    self.EXCHANGE, self.QUEUE, self.ROUTING_KEY)
        self._channel.queue_bind(callback=self.on_bindok, queue=self.QUEUE,
                                 exchange=self.EXCHANGE, routing_key=self.ROUTING_KEY)

    def on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.


        Parameters
        ----------
        method_frame: `pika.frame.Method`
            The Queue.BindOk frame
        """
        LOGGER.info('Queue bound')
        self.start_consuming()

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming.
        """
        LOGGER.info(f'Issuing consumer related RPC commands for QUEUE = {self.QUEUE}')
        self.add_on_cancel_callback()
        self._channel.basic_qos(prefetch_count=1)
        if pika.__version__ == "0.11.2":  # TODO:  Remove this when the system is upgraded to Pika 1.0.1
            self._consumer_tag = self._channel.basic_consume(self._message_callback, self.QUEUE)
        else:
            self._consumer_tag = self._channel.basic_consume(self.QUEUE, self._message_callback)

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.
        """
        LOGGER.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

        """
        LOGGER.info('Received message  %s', body)

        self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            if self._channel.is_open:
                LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
                self._channel.basic_cancel(consumer_tag=self._consumer_tag, callback=self.on_cancelok)

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        LOGGER.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        LOGGER.info('Closing the channel')
        self._channel.close()

    def start(self):
        self.run()

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        self._connection = self.connect()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        LOGGER.info(f'Stopping {self.QUEUE}')
        self._closing = True
        self.stop_consuming()
        LOGGER.info(f'Stopped {self.QUEUE}')

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        LOGGER.info('Closing connection')
        self._connection.close()
