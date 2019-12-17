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


"""NOTE:
   This file is owes a small part of its existence to the
   excellant tutorial file available on the Pika usage examples pages,
   which uses the BSD license; that is, credit the file:
   http://pika.readthedocs.org/en/0.10.0/examples/asynchronous_consumer_example.html
.
"""


import logging
import pika
from lsst.dm.csc.base.Credentials import Credentials

LOGGER = logging.getLogger(__name__)


class Declarator(object):
    """This is an example consumer that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    """
    def __init__(self, amqp_url):
        self._url = amqp_url
        connection = pika.BlockingConnection(pika.URLParameters(self._url))
        self.channel = connection.channel()
        self.channel.exchange_declare(exchange='message', exchange_type='direct', durable=True)

    def declare(self, queue, durable):
        self.channel.queue_declare(queue=queue, durable=durable)
        self.channel.queue_bind(queue=queue, exchange='message', routing_key=queue)

    def close(self):
        self.channel.close()

if __name__ == "__main__":
    cred = Credentials("iip_cred.yaml")
    user = cred.getUser("service_user")
    passwd = cred.getPasswd("service_passwd")
    url = f"amqp://{user}:{passwd}@localhost/%2ftest_at"
    
    d = Declarator(url)
    d.declare("oods_publish_to_at", True)
    d.close()
