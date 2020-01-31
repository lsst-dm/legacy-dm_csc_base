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

LOGGER = logging.getLogger(__name__)

class Beacon:
    def __init__(self, evt, scoreboard):
        self.evt = evt
        self.evt.clear()
        self.scoreboard = scoreboard

    async def ping(self, forwarder_info, seconds_to_expire, seconds_to_update):
        while True:
            if self.evt.is_set(): # if this is set, we were asked to shut down.
                LOGGER.info(f"stopping beacon for {forwarder_info.hostname}")
                self.scoreboard.delete_forwarder_association()
                return
            self.scoreboard.set_forwarder_association(forwarder_info.hostname,seconds_to_expire)
            await asyncio.sleep(seconds_to_update)

