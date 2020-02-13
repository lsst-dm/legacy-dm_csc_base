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
from lsst.dm.csc.base.archive_controller import ArchiveController

class ControllerTestChannel:
        def basic_ack(self, tag):
            pass

class ControllerMethod:
        def __init__(self):
            self.delivery_tag = 1

class ControllerTestCase(asynctest.TestCase):

    def setUp(self):
        self.loc = os.path.dirname(__file__)
        self.logname = f"test_{os.getpid()}_cred.log"
        os.environ["IIP_CONFIG_DIR"] = os.path.join(self.loc, "files", "etc", "config")
        os.environ["IIP_CREDENTIAL_DIR"] = os.path.join(self.loc, "files")

        self.controller = ArchiveController("TS", "config.yaml", self.logname)

    async def action(self, msg):
        await asyncio.sleep(1)

    async def test_controller(self):
        await self.controller.configure()
        ch = ControllerTestChannel()
        method = ControllerMethod()
    
        test1 = {}
        with self.assertRaises(Exception):
            self.controller.on_message(ch, method, None, test1)

        test2 = {'MSG_TYPE': 'test2'}
        with self.assertRaises(Exception):
            self.controller.on_message(ch, method, None, test2)

        test3 = {'MSG_TYPE': 'test3'}
        self.controller._msg_actions = {'test3': self.action}
        self.controller.on_message(ch, method, None, test3)

        await self.controller.stop_connections()

        os.unlink(os.path.join("/tmp", self.logname))

    async def test_ack_messages(self):
        await self.controller.configure()
        msg = {}
        msg['ACK_ID'] = '8675309'
        msg['JOB_NUM'] = 1
        msg['IMAGE_ID'] = 2
        msg['SESSION_ID'] = 'today'
        new_msg = self.controller.build_new_item_ack_message('/tmp', msg)

        self.assertEqual(new_msg['MSG_TYPE'], 'NEW_TS_ARCHIVE_ITEM_ACK')
        self.assertEqual(new_msg['TARGET_DIR'], '/tmp')
        self.assertEqual(new_msg['ACK_ID'], '8675309')
        self.assertEqual(new_msg['JOB_NUM'], 1)
        self.assertEqual(new_msg['IMAGE_ID'], 2)
        self.assertEqual(new_msg['SESSION_ID'], 'today')
        self.assertEqual(new_msg['COMPONENT'], 'ARCHIVE_CTRL')
        self.assertEqual(new_msg['ACK_BOOL'], 'TRUE')

        new_msg2 = self.controller.build_health_ack_message(msg)

        self.assertEqual(new_msg2['MSG_TYPE'], 'ARCHIVE_HEALTH_CHECK_ACK')
        self.assertEqual(new_msg2['COMPONENT'], 'ARCHIVE_CTRL')
        self.assertEqual(new_msg2['ACK_BOOL'], 'TRUE')
        self.assertEqual(new_msg2['ACK_ID'], '8675309')
        self.assertEqual(new_msg2['SESSION_ID'], 'today')

        msg = {}
        msg['OBSID'] = '8675309'
        msg['FILENAME'] = 'file.txt'
        msg['JOB_NUM'] = 1
        msg['SESSION_ID'] = 'today'
        new_msg3 = self.controller.build_file_transfer_completed_ack(msg)

        self.assertEqual(new_msg3['MSG_TYPE'], 'FILE_TRANSFER_COMPLETED_ACK')
        self.assertEqual(new_msg3['COMPONENT'], 'ARCHIVE_CTRL')
        self.assertEqual(new_msg3['OBSID'], '8675309')
        self.assertEqual(new_msg3['FILENAME'], 'file.txt')
        self.assertEqual(new_msg3['JOB_NUM'], 1)
        self.assertEqual(new_msg3['SESSION_ID'], 'today')

        await self.controller.stop_connections()

    async def test_target_dir(self):
        await self.controller.configure()
        target = self.controller.construct_send_target_dir("/tmp")
        os.rmdir(target)

        # this is tested twice, because it tests two paths in the code
        target = self.controller.construct_send_target_dir("/tmp")
        target = self.controller.construct_send_target_dir("/tmp")
        os.rmdir(target)

        with self.assertRaises(Exception):
            target = self.controller.construct_send_target_dir("/blah")

        await self.controller.stop_connections()

    async def test_build_file_ingest_request_message(self):
        await self.controller.configure()
        msg = {}
        msg['OBSID'] = '13'
        msg['FILENAME'] = 'filename.txt'

        new_msg = self.controller.build_file_ingest_request_message(msg)
        self.assertEqual(new_msg['MSG_TYPE'], 'TS_FILE_INGEST_REQUEST')
        self.assertEqual(new_msg['CAMERA'], 'LATISS')
        self.assertEqual(new_msg['ARCHIVER'], 'TestArchiver')
        self.assertEqual(new_msg['OBSID'], '13')
        self.assertEqual(new_msg['FILENAME'], 'filename.txt')

        await self.controller.stop_connections()

    async def test_oods_failure_message(self):
        await self.controller.configure()
        msg = {}
        msg['OBSID'] = '13'
        msg['FILENAME'] = 'filename.txt'

        new_msg = self.controller.build_oods_failure_message(msg, "description")
        self.assertEqual(new_msg['MSG_TYPE'], 'IMAGE_IN_OODS')
        self.assertEqual(new_msg['CAMERA'], 'LATISS')
        self.assertEqual(new_msg['ARCHIVER'], 'TestArchiver')
        self.assertEqual(new_msg['OBSID'], '13')
        self.assertEqual(new_msg['FILENAME'], 'filename.txt')
        self.assertEqual(new_msg['STATUS_CODE'], 1)
        self.assertEqual(new_msg['DESCRIPTION'], 'description')

        await self.controller.stop_connections()
