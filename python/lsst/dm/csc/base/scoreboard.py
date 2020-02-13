# This file is part of dm_ATArchiver
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
import redis

LOGGER = logging.getLogger(__name__)


class Scoreboard:

    def __init__(self, device, db, host, port=6379):
        LOGGER.info(f"Connecting {device} to redis database {db} at host {host}:{port}")
        self.device = device
        self.conn = redis.StrictRedis(host, port, charset='utf-8', db=db, decode_responses=True)
        self.conn.ping()

        self.STATE = "state"
        self.SESSION = "session"

    def get_state(self):
        return self.conn.hget(self.device, self.STATE)

    def set_state(self, state):
        self.conn.hset(self.device, self.STATE, state)

    def get_session(self):
        return self.conn.hget(self.device, self.SESSION)

    def set_session(self, session):
        self.conn.hset(self.device, self.SESSION, session)
