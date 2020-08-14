
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
from lsst.dm.csc.base.message_director import MessageDirector


class Parent:
    def call_fault(self, code, msg):
        pass


class Failure:
    def call_fault(self, code, msg):
        raise Exception("fail")


class MessageDirectorTestCase(asynctest.TestCase):

    def test_config_values(self):
        parent = Parent()
        logname = f"test_{os.getpid()}_event.log"
        package = lsst.utils.getPackageDir("dm_csc_base")
        os.environ["IIP_CONFIG_DIR"] = os.path.join(package, "tests", "files", "etc", "config")
        os.environ["IIP_CREDENTIAL_DIR"] = os.path.join(package, "tests", "files")
        md = MessageDirector(parent, "test", "config.yaml", logname)

        md.configure()

        self.assertEqual(md.redis_host, "localhost")
        self.assertEqual(md.redis_db, 1)
        self.assertEqual(md.forwarder_publish_queue, "test_foreman_ack_publish")
        self.assertEqual(md.archive_login, "test")
        self.assertEqual(md.archive_ip, "141.142.238.15")
        self.assertEqual(md.seconds_to_expire, 10)
        self.assertEqual(md.seconds_to_update, 3)
        self.assertEqual(md.wfs_raft, "00")
        self.assertEqual(md.wfs_ccd, ["00"])

        config = md.getConfiguration()
        root = config["ROOT"]
        val = md.config_val(root, "REDIS_HOST")
        self.assertEqual(val, "localhost")

        config = md.getConfiguration()
        val = md.config_val(root, "FAKE_KEY")

        self.assertIsNone(val)
        os.unlink(os.path.join("/tmp", logname))

    async def test_bad_connection(self):
        failure = Failure()
        logname = f"test_{os.getpid()}_bad.log"
        package = lsst.utils.getPackageDir("dm_csc_base")
        os.environ["IIP_CONFIG_DIR"] = os.path.join(package, "tests", "files", "etc", "config")
        os.environ["IIP_CREDENTIAL_DIR"] = os.path.join(package, "tests", "files")
        md = MessageDirector(failure, "test", "badhostconfig.yaml", logname)

        md.configure()
        with self.assertRaises(Exception):
            await md.start_services()
        os.unlink(os.path.join("/tmp", logname))
