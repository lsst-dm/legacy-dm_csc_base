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


class ForwarderInfo:
    """Representation of information about a Forwarder

    Parameters
    ----------
    hostname : `str`
        host where the Forwarder service is running
    ip_address : `str`
        IP address of the forwarder
    consume_queue : `str`
        RabbitMQ queue where the Forwarder service listens for incoming messages
    """

    def __init__(self, hostname, ip_address, consume_queue):
        self.hostname = hostname
        self.ip_address = ip_address
        self.consume_queue = consume_queue
