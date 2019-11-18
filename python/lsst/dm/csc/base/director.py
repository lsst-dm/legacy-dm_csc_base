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
import datetime
import logging
from lsst.dm.csc.base.base import base

LOGGER = logging.getLogger(__name__)


class Director(base):
    """Base class for methods which include transactional information
    """
    def __init__(self, name, config_filename, log_filename):
        super().__init__(name, config_filename, log_filename)

        self.initialize_session()

        cdm = self.getConfiguration()
        root = cdm["ROOT"]
        self.base_broker_addr = root["BASE_BROKER_ADDR"]

        cred = self.getCredentials()

        service_user = cred.getUser('service_user')
        service_passwd = cred.getPasswd('service_passwd')

        url = f"amqp://{service_user}:{service_passwd}@{self.base_broker_addr}"

        self.base_broker_url = url

        self._ack_lock = asyncio.Lock()
        self._ack_id = 0
        self._event_map_lock = asyncio.Lock()
        self._event_map = {}

    async def create_event(self, ack_id):
        """Create an event using the ack_id, and store it in a cache
        @param ack_id: id to use to identify event
        """
        evt = asyncio.Event()
        with await self._event_map_lock:
            self._event_map[ack_id] = evt
        return evt

    async def clear_event(self, ack_id):
        """Remove an event from the cache and clear it
        @param ack_id: id to use to identify event
        """
        evt = await self.retrieve_event(ack_id)
        if evt is None:
            LOGGER.info(f"Event does not exist: {ack_id}.")
        else:
            evt.clear()
        return evt
        
    async def retrieve_event(self, ack_id):
        """Remove an event from the cache
        @param ack_id: id to use to identify event
        """
        evt = None
        with await self._event_map_lock:
            if ack_id in self._event_map:
                evt = self._event_map.pop(ack_id)
        return evt

    async def get_next_ack_id(self):
        """Create a unique ID
        @return: a unique id
        """
        ack_id_val = 0
        with await self._ack_lock:
            self._ack_id += 1
            ack_id_val = self._ack_id
        ack_id = f"{self.session_id}_{ack_id_val}"
        return ack_id

    def initialize_session(self):
        """initialize the session id and jobnum.
        """
        self.session_id = str(datetime.datetime.now()).replace(' ','_')
        self.jobnum = 0

    def get_session_id(self):
        """Returns the session id
        @return: the current session id
        """
        return self.session_id

    def get_jobnum(self):
        """Returns the current job number
        """
        return self.jobnum

    def get_next_jobnum(self):
        """gets a new job number
        @return: a job number
        """
        self.jobnum += 1
        return self.jobnum
