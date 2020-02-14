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
from lsst.dm.csc.base.base import base

class BaseTestCase(asynctest.TestCase):

    def setUp(self):
        self.loc = os.path.dirname(__file__)
    def test_credentials(self):
        logname = f"test_{os.getpid()}_cred.log"
        os.environ["IIP_CONFIG_DIR"] = os.path.join(self.loc, "files", "etc", "config")
        os.environ["IIP_CREDENTIAL_DIR"] = os.path.join(self.loc, "files")
        b = base("test", "config.yaml", logname)
        os.unlink(os.path.join("/tmp", logname))
    
        self.assertEqual(b.getName(),"test")
        cred = b.getCredentials()
        self.assertEqual(cred.getUser("service_user"), "fakeuser")
        self.assertEqual(cred.getPasswd("service_passwd"), "fakepass")

    def test_config(self):
        logname = f"test_{os.getpid()}_config.log"
        os.environ["IIP_CONFIG_DIR"] = os.path.join(self.loc, "files", "etc", "config")
        os.environ["IIP_CREDENTIAL_DIR"] = os.path.join(self.loc, "files")
        b = base("test", "config.yaml", logname)
        config = b.getConfiguration()
        b.shutdown()
        os.unlink(os.path.join("/tmp", logname))

    def test_logdir(self):
        logname = f"test_{os.getpid()}_logdir.log"
        package = lsst.utils.getPackageDir("dm_csc_base")
        os.environ["IIP_CONFIG_DIR"] = os.path.join(package, "tests", "files", "etc", "config")
        os.environ["IIP_CREDENTIAL_DIR"] = os.path.join(package, "tests", "files")
        os.environ["IIP_LOG_DIR"] = "/tmp"
        b = base("test", "config.yaml", logname)
        config = b.getConfiguration()
        os.unlink(os.path.join("/tmp", logname))

    def test_nologdir(self):
        logname = f"test_{os.getpid()}_nologdir.log"
        package = lsst.utils.getPackageDir("dm_csc_base")
        os.environ["IIP_CONFIG_DIR"] = os.path.join(package, "tests", "files", "etc", "config")
        os.environ["IIP_CREDENTIAL_DIR"] = os.path.join(package, "tests", "files")
        if "IIP_LOG_DIR" in os.environ:
            del os.environ["IIP_LOG_DIR"]
        b = base("test", "nologconfig.yaml", logname)
        config = b.getConfiguration()

    def test_bad_config(self):
        logname = f"test_{os.getpid()}_badconfig.log"
        package = lsst.utils.getPackageDir("dm_csc_base")
        os.environ["test_DIR"] = os.path.join(package, "tests", "files")
        os.environ["IIP_CREDENTIAL_DIR"] = os.path.join(package, "tests", "files")
        b = base("test", "config.yaml", logname)
        os.unlink(os.path.join("/tmp", logname))

    def test_not_defined(self):
        logname = f"test_{os.getpid()}_badconfig.log"
        package = lsst.utils.getPackageDir("dm_csc_base")
        if "IIP_CONFIG_DIR" in os.environ:
            del os.environ["IIP_CONFIG_DIR"]
        if "test_DIR" in os.environ:
            del os.environ["test_DIR"]
        with self.assertRaises(Exception):
            b = base("test", "config.yaml", logname)

    def test_bad_cred_dir(self):
        logname = f"test_{os.getpid()}_badconfig.log"
        package = lsst.utils.getPackageDir("dm_csc_base")
        os.environ["test_DIR"] = os.path.join(package, "tests", "files")
        os.environ["IIP_CREDENTIAL_DIR"] = os.path.join(package, "tests", "files", "baddir")
        with self.assertRaises(PermissionError):
            b = base("test", "config.yaml", logname)
        os.unlink(os.path.join("/tmp", logname))

    def test_missing_config(self):
        logname = f"test_{os.getpid()}_config.log"
        package = lsst.utils.getPackageDir("dm_csc_base")
        os.environ["IIP_CONFIG_DIR"] = os.path.join(package, "tests", "files", "etc", "config")
        os.environ["IIP_CREDENTIAL_DIR"] = os.path.join(package, "tests", "files")
        with self.assertRaises(FileNotFoundError):
            b = base("test", "missing_config.yaml", logname)
