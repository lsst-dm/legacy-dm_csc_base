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
import asynctest

from lsst.dm.csc.base.archiveboard import Archiveboard
from lsst.dm.csc.base.forwarder_info import ForwarderInfo


class ArchiveboardTestCase(asynctest.TestCase):

    def test_archiveboard(self):
        ab = Archiveboard("AT", 1, "localhost")
        ab.conn.delete('forwarder_list')

        ab.set_jobnum('123')
        s = ab.get_jobnum()
        self.assertEqual(s, '123')

        info1 = ForwarderInfo("localhost", "127.0.0.1", None)
        ab.push_forwarder_onto_list(info1)
        info2 = ab.pop_forwarder_from_list()
        self.assertEqual(info1.hostname, info2.hostname)
        self.assertEqual(info1.ip_address, info2.ip_address)
        self.assertEqual(info1.consume_queue, info2.consume_queue)

        with self.assertRaises(RuntimeError):
            ab.pop_forwarder_from_list()

        ab.set_paired_forwarder_info(info1, 10)
        info4 = ab.get_paired_forwarder_info()
        self.assertEqual(info1.hostname, info4.hostname)
        self.assertEqual(info1.ip_address, info4.ip_address)
        self.assertEqual(info1.consume_queue, info4.consume_queue)

        info5 = ab.create_forwarder_info({})
        self.assertIsNone(info5)

        ab.set_forwarder_association("myhost", 10)

        val = ab.check_forwarder_presence("AT_association")
        self.assertEqual(val, "myhost")

        ab.delete_forwarder_association()
