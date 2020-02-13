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
import os
import unittest
import asynctest

import lsst.utils.tests
from lsst.dm.csc.base.forwarder_info import ForwarderInfo

class ForwarderInfoTestCase(asynctest.TestCase):

    def test_forwarder_info(self):
        hostname = "localhost"
        ip_address = "127.0.0.1"
        consume_queue = "fake_queue"

        f = ForwarderInfo(hostname, ip_address, consume_queue)
        self.assertEqual(hostname, f.hostname)
        self.assertEqual(ip_address, f.ip_address)
        self.assertEqual(consume_queue, f.consume_queue)
