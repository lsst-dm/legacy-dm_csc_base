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

import asyncio
import logging
import pika
from pprint import pformat
from lsst.ctrl.iip.YamlHandler import YamlHandler
from pika.adapters.asyncio_connection import AsyncioConnection

LOGGER = logging.getLogger(__name__)


class Publisher(object):

    def __init__(self, amqp_url, parent=None, logger_level=LOGGER.info):

        # only emit logging messages from pika and WARNING and above
        logging.getLogger("pika").setLevel(logging.WARNING)
        self._connection = None
        self._channel = None
        self._url = amqp_url
        self.setup_complete_event = asyncio.Event()
        self.setup_complete_event.clear()

        self._message_handler = YamlHandler()
        self._stopping = False

        self.parent = parent
        self.logger_level = logger_level

    def connect(self):
        self._connection = AsyncioConnection(pika.URLParameters(self._url),
                                             on_open_callback=self.on_connection_open,
                                             on_close_callback=self.on_connection_closed,
                                             on_open_error_callback=self.on_connection_open_error)

    def on_connection_open(self, connection):
        LOGGER.info("connection opened")
        self._connection = connection
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.

        :param pika.adapters.asyncio_connection.AsyncioConnection _unused_connection:
           The connection
        :param Exception err: The error

        """
        LOGGER.error(f'Connection open failed: {err}')
        self.parent.fault(5071, f'Connection open failed: {err}')

    def open_channel(self):
        LOGGER.info("creating a new channel")
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        LOGGER.info("channel opened")
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_complete_event.set()

    def add_on_channel_close_callback(self):
        LOGGER.info('adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        LOGGER.info('Channel %i was closed %s' % (channel, reason))
        self._channel = None
        self._connection.close()

    def on_connection_closed(self, connection, reason):
        """
        Params:
            connection - closed object connection
            reason - reason for closure
        """
        LOGGER.info("on_connection_closed called")
        self._channel = None
        self._connect = None

    async def close(self):
        if self._channel is not None:
            self._channel.close()

    async def publish_message(self, route_key, msg):

        encoded_data = self._message_handler.encode_message(msg)

        self.logger_level("Sending msg to %s", route_key)

        # Since this is asynchronous, it's possible to still be in the
        # process of getting setup and having self._channel be None when
        # publish_message is being called from another thread, so we wait
        # here until setup is completed.
        await self.setup_complete_event.wait()
        self._channel.basic_publish(exchange='message', routing_key=route_key, body=encoded_data)
        self.logger_level('message sent message body is: %s', pformat(str(msg)))

    async def stop(self):
        self._stopping = True
        await self.close()
        LOGGER.info('Stopped')

    async def start(self):
        try:
            await self.connect()
        except Exception:
            LOGGER.error('No channel - connection channel is None')
