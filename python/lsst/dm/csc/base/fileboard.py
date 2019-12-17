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
import redis

LOGGER = logging.getLogger(__name__)


class Fileboard:

    def __init__(self, db, host, port=6379):
        LOGGER.info(f"Connecting to redis database {db} at host {host}:{port}")
        self.todo = "todo"
        self.completed = "completed"
        self.conn = redis.StrictRedis(host, port, charset='utf-8', db=db, decode_responses=True)

    def push_todo(self, camera, archiver, obsid, filename):
        d = {'camera': camera, 'archiver': archiver, 'obsid': image_name, 'filename': filename}
        self.conn.rpush(self.todo, d)

    def pop_todo(self, timeout=0):
        return self.conn.blpop(self.todo, timeout)

if __name__ == "__main__":
    board = Fileboard(10, "localhost")
    board.push_todo("LATISS", "ATArchiver", "myfile", "/tmp/myfile")
    d = board.pop_todo()
    print(d[1])
    d = board.pop_todo(5)
    print(d)

