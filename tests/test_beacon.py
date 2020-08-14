# This file is part of dm_csc_base
#
# Developed for the LSST Telescope and Site Systems.
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
import asynctest

from lsst.dm.csc.base.beacon import Beacon
from lsst.dm.csc.base.forwarder_info import ForwarderInfo


class BeaconScoreboard:
    def set_forwarder_association(self, hostname, expire):
        return

    def delete_forwarder_association(self):
        return


class WatcherTestCase(asynctest.TestCase):

    async def test_beacon(self):
        evt = asyncio.Event()
        board = BeaconScoreboard()
        info = ForwarderInfo("localhost", "127.0.0.1", None)

        beacon = Beacon(evt, board)
        asyncio.create_task(self.pause(evt, 2))
        await beacon.ping(info, 5, 1)

    async def pause(self, evt, seconds):
        await asyncio.sleep(seconds)
        evt.set()
