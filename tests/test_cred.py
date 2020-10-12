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
import yaml

import lsst.utils.tests
from lsst.dm.csc.base.Credentials import Credentials


class CredentialsTestCase(asynctest.TestCase):

    def setUp(self):
        self.package = lsst.utils.getPackageDir("dm_csc_base")
        os.environ["IIP_CREDENTIAL_DIR"] = os.path.join(self.package, "tests", "files")

    def test_credentials(self):
        cred = Credentials("iip_cred.yaml")
        self.assertEqual(cred.getUser("service_user"), "fakeuser")
        self.assertEqual(cred.getPasswd("service_passwd"), "fakepass")

    def test_missing_credentials(self):
        del os.environ["IIP_CREDENTIAL_DIR"]
        os.environ["HOME"] = "/tmp"
        with self.assertRaises(PermissionError):
            Credentials("iip_cred.yaml")
        del os.environ["HOME"]

    def test_bad_permission_dir(self):
        os.environ["IIP_CREDENTIAL_DIR"] = os.path.join(self.package, "tests", "files", "badetc")
        with self.assertRaises(PermissionError):
            Credentials("iip_cred.yaml")

    def test_bad_dir(self):
        os.environ["IIP_CREDENTIAL_DIR"] = os.path.join(self.package, "tests", "files", "nothing")
        with self.assertRaises(PermissionError):
            Credentials("iip_cred.yaml")

    def test_bad_permissions_file(self):
        with self.assertRaises(PermissionError):
            Credentials("bad_permissions.yaml")

    def test_missing_credential_file(self):
        with self.assertRaises(FileNotFoundError):
            Credentials("nonexistant.yaml")

    def test_bad_yaml_file(self):
        with self.assertRaises(yaml.YAMLError):
            Credentials("bad_yaml_file.yaml")
