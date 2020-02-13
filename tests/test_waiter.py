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
import os
import unittest
import asynctest
import pytest

import lsst.utils.tests
from lsst.dm.csc.base.waiter import Waiter

class Parent:
    def call_fault(self, code, report):
        raise Exception("exception")

class WaiterTestCase(asynctest.TestCase):

    async def test_waiter(self):
        evt = asyncio.Event()
        parent = Parent()
        w = Waiter(evt, parent, 1)
        with self.assertRaises(Exception):
            await w.pause(1, "report placeholder")

    async def test_waiter_interrupted(self):
        evt = asyncio.Event()
        parent = Parent()
        w = Waiter(evt, parent, 2)
        evt.clear()
        await w.pause(1, "report placeholder")

