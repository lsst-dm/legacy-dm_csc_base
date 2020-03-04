# This file is part of dm_csc_base
#
# Developed for the LSST Data Management System.
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
from copy import deepcopy
import datetime
import logging
import os
import os.path
import sys
from lsst.dm.csc.base.consumer import Consumer
from lsst.dm.csc.base.publisher import Publisher
from lsst.dm.csc.base.base import base

LOGGER = logging.getLogger(__name__)


class ArchiveController(base):
    def __init__(self, name, config_filename, log_filename):
        super().__init__(name, config_filename, log_filename)

    async def configure(self):
        cdm = self.getConfiguration()
        root = cdm['ROOT']
        redis_host = root['REDIS_HOST']
        redis_db = root['ARCHIVER_REDIS_DB']
        self.forwarder_publish_queue = root['FORWARDER_PUBLISH_QUEUE']
        self.oods_publish_queue = root['OODS_PUBLISH_QUEUE']
        self.archive_ctrl_publish_queue = root['ARCHIVE_CTRL_PUBLISH_QUEUE']
        self.archive_ctrl_consume_queue = root['ARCHIVE_CTRL_CONSUME_QUEUE']
        self.camera_name = root['CAMERA_NAME']
        self.archiver_name = root['ARCHIVER_NAME']
        self.short_name = root['SHORT_NAME']

        archive = root['ARCHIVE']

        if 'FORWARDER_STAGING' in archive:
            self.forwarder_staging_dir = archive['FORWARDER_STAGING']
            LOGGER.info(f"forwarder will stage to {self.forwarder_staging_dir}")
        else:
            msg = "ARCHIVE.FORWARDER_STAGING does not exist in configuration file"
            LOGGER.warn(msg)
            sys.exit(0)
        self.oods_staging_dir = None
        if 'OODS_STAGING' in archive:
            self.oods_staging_dir = archive['OODS_STAGING']
            LOGGER.info(f"oods files will be staged to {self.oods_staging_dir}")
        else:
            msg = "ARCHIVE.OODS_STAGING does not exist in config file; will not link for OODS"
            LOGGER.warn(msg)

        self.dbb_staging_dir = None
        if 'DBB_STAGING' in archive:
            self.dbb_staging_dir = archive['DBB_STAGING']
            LOGGER.info(f"dbb files will be staged to {self.dbb_staging_dir}")
        else:
            msg = "ARCHIVE.DBB_STAGING does not exist in config file; will not link for DBB"
            LOGGER.warn(msg)

        self.base_broker_addr = root["BASE_BROKER_ADDR"]

        cred = self.getCredentials()

        service_user = cred.getUser('service_user')
        service_passwd = cred.getPasswd('service_passwd')

        self.base_broker_url = f"amqp://{service_user}:{service_passwd}@{self.base_broker_addr}"

        dir_list = [self.forwarder_staging_dir, self.oods_staging_dir, self.dbb_staging_dir]
        self.create_directories(dir_list)
        await self.setup_consumers()
        await self.setup_publishers()
        return self

    def create_directories(self, dir_list):
        for directory in dir_list:
            if directory is not None:
                os.makedirs(os.path.dirname(directory), exist_ok=True)

    async def setup_publishers(self):
        LOGGER.info("Setting up ArchiveController publisher")
        self.publisher = Publisher(self.base_broker_url, csc_parent=None,  logger_level=LOGGER.debug)
        await self.publisher.start()

    async def stop_publishers(self):
        LOGGER.info("stopping publishers")
        if self.publisher is not None:
            await self.publisher.stop()

    async def setup_consumers(self):
        """ Create ThreadManager object with base broker url and kwargs to setup consumers.

            :params: None.

            :return: None.
        """

        # messages from ArchiverCSC and Forwarder
        self.consumer = Consumer(self.base_broker_url, None, self.archive_ctrl_consume_queue,
                                 self.on_message)
        self.consumer.start()

    def stop_consumers(self):
        LOGGER.info("stopping publishers")
        if self.consumer is not None:
            self.consumer.stop()
            self.consumer = None

    async def stop_connections(self):
        await self.stop_publishers()
        self.stop_consumers()

    def on_message(self, ch, method, properties, body):
        if 'MSG_TYPE' not in body:
            msg = f"received invalid message: {body}"
            LOGGER.warning(msg)
            raise Exception(msg)
        msg_type = body['MSG_TYPE']
        if msg_type not in self._msg_actions:
            msg = f"{msg_type} was not found in msg_action list"
            LOGGER.warning(msg)
            raise Exception(msg)
        if msg_type != 'ARCHIVE_HEALTH_CHECK':
            LOGGER.info("received message")
            LOGGER.info(body)
        ch.basic_ack(method.delivery_tag)
        handler = self._msg_actions.get(body['MSG_TYPE'])

        loop = asyncio.get_event_loop()
        task = loop.create_task(handler(body))

    def build_new_item_ack_message(self, target_dir, incoming_msg):
        d = {}
        d['MSG_TYPE'] = f'NEW_{self.short_name}_ARCHIVE_ITEM_ACK'
        d['TARGET_DIR'] = target_dir
        d['ACK_ID'] = incoming_msg['ACK_ID']
        d['JOB_NUM'] = incoming_msg['JOB_NUM']
        d['IMAGE_ID'] = incoming_msg['IMAGE_ID']
        d['COMPONENT'] = 'ARCHIVE_CTRL'
        d['ACK_BOOL'] = 'TRUE'
        d['SESSION_ID'] = incoming_msg['SESSION_ID']
        return d

    def build_health_ack_message(self, incoming_msg):
        d = {}
        d['MSG_TYPE'] = "ARCHIVE_HEALTH_CHECK_ACK"
        d['COMPONENT'] = 'ARCHIVE_CTRL'
        d['ACK_BOOL'] = "TRUE"
        d['ACK_ID'] = incoming_msg['ACK_ID']
        d['SESSION_ID'] = incoming_msg['SESSION_ID']
        return d

    def construct_send_target_dir(self, target_dir):
        observing_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=12)
        day_string = str(observing_time.date())

        final_target_dir = f"{target_dir}/{day_string}/"

        # This code allows two users belonging to the same group (such as ARCHIVE)
        # to both create and write to a specific directory.
        # The common group must be made the primary group for both users like this:
        # usermod -g ARCHIVE ATS_user
        # and the sticky bit must be set when the group is created.
        # chmod is called after creation to deal with system umask
        if os.path.isdir(final_target_dir):
            pass
        else:
            try:
                os.mkdir(final_target_dir, 0o2775)
            except Exception as e:
                msg = f'failure to create {final_target_dir}: {e}'
                LOGGER.error(msg)
                raise Exception(msg)
            os.chmod(final_target_dir, 0o775)

        return final_target_dir

    async def process_health_check(self, msg):
        ack_msg = self.build_health_ack_message(msg)
        await self.publisher.publish_message(self.forwarder_publish_queue, ack_msg)

    async def process_new_archive_item(self, msg):
        # send this to the archive staging area
        target_dir = self.construct_send_target_dir(self.forwarder_staging_dir)

        ack_msg = self.build_new_item_ack_message(target_dir, msg)

        reply_queue = msg['REPLY_QUEUE']
        LOGGER.info(ack_msg)
        await self.publisher.publish_message(reply_queue, ack_msg)

    def build_file_transfer_completed_ack(self, incoming_msg):
        LOGGER.info(f"data was: {incoming_msg}")
        d = {}
        d['MSG_TYPE'] = 'FILE_TRANSFER_COMPLETED_ACK'
        d['COMPONENT'] = 'ARCHIVE_CTRL'
        d['OBSID'] = incoming_msg['OBSID']
        d['FILENAME'] = incoming_msg['FILENAME']
        d['JOB_NUM'] = incoming_msg['JOB_NUM']
        d['SESSION_ID'] = incoming_msg['SESSION_ID']
        return d

    async def process_file_transfer_completed(self, incoming_msg):
        msg = deepcopy(incoming_msg)
        filename = incoming_msg['FILENAME']
        reply_queue = incoming_msg['REPLY_QUEUE']
        ack_msg = self.build_file_transfer_completed_ack(incoming_msg)
        LOGGER.info(ack_msg)
        await self.publisher.publish_message(reply_queue, ack_msg)

        # try and create a link to the file
        try:
            dbb_file, oods_file = self.create_links_to_file(filename)
        except Exception as e:
            LOGGER.info(f'{e}')
            # send an error that an error occurred trying to set up for the ingest into the OODS
            err = f"Couldn't create link for OODS: {e}"
            task = asyncio.create_task(self.send_oods_failure_message(msg, err))
            return
        # send an message to the OODS to ingest the file
        msg['FILENAME'] = oods_file
        task = asyncio.create_task(self.send_ingest_message_to_oods(msg))

    def create_link_to_file(self, filename, dirname):
        # remove the staging area portion from the filepath
        basefile = filename.replace(self.forwarder_staging_dir, '').lstrip('/')

        # create a new full path to where the file will be linked for the OODS
        new_file = os.path.join(dirname, basefile)

        # hard link the file in the staging area
        # create the directory path where the file will be linked for the OODS
        new_dir = os.path.dirname(new_file)
        try:
            os.makedirs(new_dir, exist_ok=True)
            # hard link the file in the staging area
            os.link(filename, new_file)
            LOGGER.info(f"created link to {new_file}")
        except Exception as e:
            LOGGER.info(f"error trying to create link to {new_file} {e}")
            return None

        return new_file

    def create_links_to_file(self, forwarder_filename):

        if self.dbb_staging_dir is not None:
            dbb_file = self.create_link_to_file(forwarder_filename, self.dbb_staging_dir)

        if self.oods_staging_dir is not None:
            oods_file = self.create_link_to_file(forwarder_filename, self.oods_staging_dir)

        if (dbb_file is not None) and (oods_file is not None):
            # remove the original file, since we've linked it
            LOGGER.info(f"links were created successfully; removing {forwarder_filename}")
            os.unlink(forwarder_filename)

        return dbb_file, oods_file

    async def send_oods_failure_message(self, body, description):
        """Send a message to the Archiver that we failed to ingest into the OODS."""
        msg = self.build_oods_failure_message(body, description)
        await self.publisher.publish_message(self.archive_ctrl_publish_queue, msg)

    async def send_ingest_message_to_oods(self, body):
        """Send a message to the OODS to perform an ingest"""
        msg = self.build_file_ingest_request_message(body)
        LOGGER.info(f"sending ingest message to oods: {msg}")
        await self.publisher.publish_message(self.oods_publish_queue, msg)

    def build_file_ingest_request_message(self, msg):
        d = {}
        d['MSG_TYPE'] = f'{self.short_name}_FILE_INGEST_REQUEST'
        d['CAMERA'] = self.camera_name
        d['ARCHIVER'] = self.archiver_name
        d['OBSID'] = msg['OBSID']
        d['FILENAME'] = msg['FILENAME']
        return d

    def build_oods_failure_message(self, msg, description):
        d = {}
        d['MSG_TYPE'] = 'IMAGE_IN_OODS'
        d['CAMERA'] = self.camera_name
        d['ARCHIVER'] = self.archiver_name
        d['OBSID'] = msg['OBSID']
        d['FILENAME'] = msg['FILENAME']
        d['STATUS_CODE'] = 1
        d['DESCRIPTION'] = description
        return d
