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

import json
import logging
from lsst.dm.csc.base.forwarder_info import ForwarderInfo
from lsst.dm.csc.base.scoreboard import Scoreboard

LOGGER = logging.getLogger(__name__)


class Archiveboard(Scoreboard):
    """Archiver scoreboard to communicate with Redis database

    Parameters
    ----------
    device : `str`
    db : `int`
    host : `str`
        host name of the Redis database server
    port : `int`
        Redis database service network port number
    key : `str`
        association key
    """

    def __init__(self, device, db, host, port=6379, key=None):
        super().__init__(device, db, host, port)

        self.association_key = key
        self.JOBNUM = "jobnum"
        self.PAIRED_FORWARDER = "paired_forwarder"
        self.FORWARDER_LIST = "forwarder_list"

    def get_jobnum(self):
        """retrieve the job number from the redis database
        """
        return self.conn.hget(self.device, self.JOBNUM)

    def set_jobnum(self, jobnum):
        """set the job number in the redis database

        Parameters
        ----------
        jobnum : `int`
            The job number
        """
        self.conn.hset(self.device, self.JOBNUM, jobnum)

    def pop_forwarder_from_list(self):
        """Pop an available forwarder from the redis database list
        """
        LOGGER.info(f"popping from {self.FORWARDER_LIST}")
        data = self.conn.brpop(self.FORWARDER_LIST, 1)
        if data is None:
            LOGGER.info("No forwarder available on scoreboard list")
            raise RuntimeError("No forwarder available on scoreboard list")
        item = data[1]
        d = json.loads(item)

        return self.create_forwarder_info(d)

    def push_forwarder_onto_list(self, forwarder_info):
        """Add the contents of the forwarder_info object to the Forwarder list in the Redis database

        Parameters
        ----------
        forwarder_info : `lsst.dm.csc.base.forwarder_info.ForwarderInfo`
        """
        info = forwarder_info.__dict__
        data = json.dumps(info)
        self.conn.lpush(self.FORWARDER_LIST, data)

    def get_paired_forwarder_info(self):
        """Retrieve the paired Forwarder as an ForwarderInfo object from the Redis databse

        Returns
        -------
        forwarder_info : `lsst.dm.csc.base.forwarder_info.ForwarderInfo`
        """
        data = self.conn.hget(self.device, self.PAIRED_FORWARDER)
        d = json.loads(data)
        return self.create_forwarder_info(d)

    def create_forwarder_info(self, forwarder):
        """Take the contents of a dictionary and return a Forwarderinfo object

        Parameters
        ----------
        forwarder : `dict`

        Returns
        -------
        forwarder_info : `lsst.dm.csc.base.forwarder_info.ForwarderInfo`
        """
        try:
            hostname = forwarder['hostname']
            ip_address = forwarder['ip_address']
            consume_queue = forwarder['consume_queue']
            forwarder_info = ForwarderInfo(hostname=hostname, ip_address=ip_address, consume_queue=consume_queue)
            return forwarder_info
        except Exception as e:
            LOGGER.info("Exception: "+str(e))
            return None

    def check_forwarder_presence(self, forwarder_key):
        """Check for the presence of the given key

        This key is updated by the remote service at regular intervals.  If
        it fails to update, the value is timed out of existence by Redis and
        will indicate the service has either died or can no longer communicate


        Parameters
        ----------
        forwarder_key : `str`
            Forwarder key used for looking up service existence

        Returns
        -------
        The value of the key entry
        """
        return self.conn.get(forwarder_key)

    def set_forwarder_association(self, forwarder_hostname, timeout):
        """Set the forwarder hostname to which the Archiver is associated

        Parameters
        ----------
        forwarder_hostname : `str`
            the host name on which the forwarder service is running
        timeout : `int`
            timeout value
        """
        self.conn.set(self.association_key, forwarder_hostname, timeout)

    def delete_forwarder_association(self):
        """Delete the forwarder association key
        """
        LOGGER.info(f'deleting {self.association_key}')
        self.conn.delete(self.association_key)

    def set_paired_forwarder_info(self, forwarder_info, timeout):
        """Add the paired forwarder's information

        Parameters
        ----------
        forwarder_info : `lsst.dm.csc.base.forwarder_info.ForwarderInfo`
            Forwarder information object
        timeout : `int`
            timeout value
        """
        info = forwarder_info.__dict__
        data = json.dumps(info)
        self.conn.hset(self.device, self.PAIRED_FORWARDER, data)
