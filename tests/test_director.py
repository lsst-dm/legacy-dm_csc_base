
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
import asynctest

import lsst.utils.tests
from lsst.dm.csc.base.director import Director


class DirectorTestCase(asynctest.TestCase):

    async def test_event(self):
        logname = f"test_{os.getpid()}_event.log"
        package = lsst.utils.getPackageDir("dm_csc_base")
        os.environ["IIP_CONFIG_DIR"] = os.path.join(package, "tests", "files", "etc", "config")
        os.environ["IIP_CREDENTIAL_DIR"] = os.path.join(package, "tests", "files")
        d = Director("test", "config.yaml", logname)

        evt1 = await d.create_event("id1")
        evt2 = await d.retrieve_event("id1")
        self.assertEqual(evt1, evt2)

        evt3 = await d.clear_event("id2")
        self.assertFalse(evt3.is_set())

        evt4 = await d.clear_event("id3")
        self.assertIsNone(evt4)
        os.unlink(os.path.join("/tmp", logname))

    async def test_ack_id(self):
        logname = f"test_{os.getpid()}_ack_id.log"
        package = lsst.utils.getPackageDir("dm_csc_base")
        os.environ["IIP_CONFIG_DIR"] = os.path.join(package, "tests", "files", "etc", "config")
        os.environ["IIP_CREDENTIAL_DIR"] = os.path.join(package, "tests", "files")
        d = Director("test", "config.yaml", logname)

        session_id = d.get_session_id()

        val = await d.get_next_ack_id()
        self.assertEqual(val, f"{session_id}_1")

        val = await d.get_next_ack_id()
        self.assertEqual(val, f"{session_id}_2")
        os.unlink(os.path.join("/tmp", logname))

    def test_jobnum(self):
        logname = f"test_{os.getpid()}_jobnum.log"
        package = lsst.utils.getPackageDir("dm_csc_base")
        os.environ["IIP_CONFIG_DIR"] = os.path.join(package, "tests", "files", "etc", "config")
        os.environ["IIP_CREDENTIAL_DIR"] = os.path.join(package, "tests", "files")
        d = Director("test", "config.yaml", logname)

        val = d.get_jobnum()
        self.assertEqual(val, 0)

        val = d.get_next_jobnum()
        self.assertEqual(val, 1)

        val = d.get_jobnum()
        self.assertEqual(val, 1)
        os.unlink(os.path.join("/tmp", logname))
