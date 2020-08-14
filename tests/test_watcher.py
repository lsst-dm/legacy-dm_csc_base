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

from lsst.dm.csc.base.watcher import Watcher


class WatcherParent:
    def call_fault(self, code, report):
        raise Exception("exception")


class WatcherParent2:
    def call_fault(self, code, report):
        return


class WatcherScoreboard1:
    def check_forwarder_presence(self, key):
        return self


class WatcherScoreboard2:
    def check_forwarder_presence(self, key):
        return None


class WatcherTestCase(asynctest.TestCase):

    async def test_watcher(self):
        evt = asyncio.Event()
        parent = WatcherParent2()
        board = WatcherScoreboard1()

        w = Watcher(evt, parent, board)
        asyncio.create_task(self.pause(evt, 2))
        await w.peek(None, 1)

    async def pause(self, evt, seconds):
        await asyncio.sleep(seconds)
        evt.set()

    async def test_watcher_interrupted1(self):
        evt = asyncio.Event()
        parent = WatcherParent()
        board = WatcherScoreboard2()

        w = Watcher(evt, parent, board)
        with self.assertRaises(Exception):
            await w.peek(None, 1)

    async def test_watcher_interrupted2(self):
        evt = asyncio.Event()
        parent = WatcherParent2()
        board = WatcherScoreboard2()

        w = Watcher(evt, parent, board)
        await w.peek(None, 1)
