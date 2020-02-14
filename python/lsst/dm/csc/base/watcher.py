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

class Watcher:
    def __init__(self, evt, parent, scoreboard):
        self.evt = evt
        self.evt.clear()
        self.parent = parent
        self.scoreboard = scoreboard

    async def peek(self, forwarder_key, seconds_until_next_peek):
        while True:
            if self.evt.is_set():  # if this is set, we were asked to shut down.
                return
            if self.scoreboard.check_forwarder_presence(forwarder_key) is None:
                code = 5755
                report = "Forwarder is does not appear to be alive.  Going into fault state."
                self.parent.call_fault(code=code, report=report)
                return
            await asyncio.sleep(seconds_until_next_peek)
