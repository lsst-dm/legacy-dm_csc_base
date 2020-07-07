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
import logging
import traceback
from lsst.dm.csc.base.publisher import Publisher
from lsst.dm.csc.base.consumer import Consumer
from lsst.dm.csc.base.director import Director
from lsst.dm.csc.base.waiter import Waiter
from lsst.dm.csc.base.beacon import Beacon
from lsst.dm.csc.base.watcher import Watcher
from lsst.dm.csc.base.archiveboard import Archiveboard

LOGGER = logging.getLogger(__name__)


class MessageDirector(Director):
    """ Specialization of Director to handle non-CSC messaging and transactions
    """
    def __init__(self, parent, name, config_filename, log_filename):
        super().__init__(name, config_filename, log_filename)
        self.parent = parent

        self._msg_actions = {}

        config = self.getConfiguration()

        self.ARCHIVE_CONTROLLER_NAME = None
        self.FWDR_HEALTH_CHECK_ACK = None
        self.CAMERA_NAME = None
        self.ARCHIVER_NAME = None
        self.SHORT_NAME = None
        self.ASSOCIATION_KEY = None

        self.FILE_INGEST_REQUEST = None
        self.NEW_ARCHIVE_ITEM = None
        self.FWDR_XFER_PARAMS = None
        self.FWDR_END_READOUT = None
        self.FWDR_HEADER_READY = None

        self.OODS_CONSUME_QUEUE = None
        self.OODS_PUBLISH_QUEUE = None
        self.ARCHIVE_CTRL_PUBLISH_QUEUE = None
        self.ARCHIVE_CTRL_CONSUME_QUEUE = None
        self.TELEMETRY_QUEUE = None

        # if wait_for_ack_timeouts is set to True, then a timer is started 
        # when a message is sent to the Forwarder.  The Forwarder is exxpected
        # to respond within that timeout window or the archiver will go into
        # a fault state
        self.wait_for_ack_timeouts = False

    def config_val(self, config, key):
        if key in config:
            return config[key]
        LOGGER.warning(f'configuration key {key} not found; setting to None')
        return None

    def configure(self):

        cdm = self.getConfiguration()

        LOGGER.info(f'******* Initializing {self.ARCHIVER_NAME} ******')
        root = cdm["ROOT"]
        print(f"root = {root}")

        if "ACK_TIMEOUT" in root:
            self.ack_timeout = root["ACK_TIMEOUT"]
            LOGGER.info(f'message ack timeout set to {self.ack_timeout}')
        else:
            self.ack_timeout = 5
            LOGGER.info(f'ACK_TIMEOUT not specified in configuation file; default message ack timeout set to {self.ack_timeout}')

        self.redis_host = root["REDIS_HOST"]
        self.redis_db = root["ARCHIVER_REDIS_DB"]

        self.forwarder_publish_queue = root["FORWARDER_PUBLISH_QUEUE"]
        self.forwarder_host = None

        archive = root['ARCHIVE']
        self.archive_login = archive['ARCHIVE_LOGIN']
        self.archive_ip = archive['ARCHIVE_IP']

        csc = root['CSC']
        beacon = csc['BEACON']
        self.seconds_to_expire = beacon['SECONDS_TO_EXPIRE']
        self.seconds_to_update = beacon['SECONDS_TO_UPDATE']

        ats = root['ATS']
        self.wfs_raft = ats['WFS_RAFT']
        self.wfs_ccd = ats['WFS_CCD']

        self.archive_heartbeat_task = None

        self.startIntegration_evt = asyncio.Event()
        self.endReadout_evt = asyncio.Event()
        self.largeFileObjectAvailable_evt = asyncio.Event()

        self.archive_heartbeat_evt = asyncio.Event()

        self.services_started_evt = asyncio.Event()

        self.stop_forwarder_beacon_evt = asyncio.Event()
        self.stop_watcher_evt = asyncio.Event()

        self.publisher = None
        self.oods_consumer = None
        self.archive_consumer = None
        self.forwarder_consumer = None
        self.telemetry_consumer = None

        self.scoreboard = None

    async def start_services(self):
        """ called when CSC start commmand is made to start beacons, scoreboard and other services
        """
        # this is the beginning of the start command, when we're about to go into disabled state
        LOGGER.info("start_services called")

        self.services_started_evt.set()
        try:
            self.scoreboard = Archiveboard(self._name, db=self.redis_db, host=self.redis_host, key=self.ASSOCIATION_KEY)
        except Exception as e:
            LOGGER.info(e)
            msg = "scoreboard could't establish connection with redis broker"
            self.parent.call_fault(5701, msg)
            return

        forwarder_info = None
        try:
            forwarder_info = self.scoreboard.pop_forwarder_from_list()

            LOGGER.info(f"pairing with forwarder {forwarder_info.hostname}")
        except Exception as e:
            msg = f"no forwarder on forwarder_list in redis db {self.redis_db} on {self.redis_host}"
            self.parent.call_fault(5701, msg)
            return
        try:
            # record which forwarder we're paired to
            self.beacon = Beacon(self.stop_forwarder_beacon_evt, self.scoreboard)
            self.beacon_task = asyncio.create_task(self.beacon.ping(forwarder_info,
                                                                    self.seconds_to_expire,
                                                                    self.seconds_to_update))
        except Exception as e:
            print(e)

        self.forwarder_host = forwarder_info.hostname
        self.forwarder_consume_queue = forwarder_info.consume_queue
        await self.establish_connections(forwarder_info)

        task = asyncio.create_task(self.send_association_message())
        await task

    async def stop_services(self):
        """ Stop all non-CSC commmunication
        """
        if self.services_started_evt.is_set():
            self.stop_watcher_evt.set()
            self.stop_forwarder_beacon_evt.set()
            await self.rescind_connections()
            self.services_started_evt.clear()

    async def establish_connections(self, info):
        """ Establish non-CSC messaging connections
        """
        await self.setup_publishers()
        await self.setup_consumers()

        self.archive_heartbeat_task = asyncio.create_task(self.emit_heartbeat(self.ARCHIVE_CONTROLLER_NAME,
                                                                               self.ARCHIVE_CTRL_CONSUME_QUEUE,
                                                                               "ARCHIVE_HEALTH_CHECK",
                                                                               self.archive_heartbeat_evt))
    async def rescind_connections(self):
        """ Stop non-CSC messaging connections
        """
        LOGGER.info("rescinding connections")
        await self.stop_heartbeats()
        await self.stop_publishers()
        await self.stop_consumers()
        LOGGER.info("all connections rescinded")

    async def stop_heartbeats(self):
        """ Stop all heartbeat tasks
        """
        LOGGER.info("stopping heartbeats")
        await self.stop_heartbeat_task(self.archive_heartbeat_task)

    async def stop_heartbeat_task(self, task):
        """ Stop a specific heartbeat task
        @param task: the task to stop
        """
        if task is not None:
            task.cancel()
            await task

    async def setup_publishers(self):
        """ Set up base publisher with pub_base_broker_url by creating a new instance
            of AsyncioPublisher class

            :params: None.

            :return: None.
        """
        LOGGER.info('Setting up archiver publisher')
        self.publisher = Publisher(self.base_broker_url, csc_parent=self.parent)
        await self.publisher.start()

    async def stop_publishers(self):
        """ Stop all publisher connections
        """
        LOGGER.info("stopping publishers")
        if self.publisher is not None:
            await self.publisher.stop()

    async def setup_consumers(self):
        """ Create ThreadManager object with base broker url and kwargs to setup consumers.

            :params: None.

            :return: None.
        """
        # message from OODS
        self.oods_consumer = Consumer(self.base_broker_url, self.parent, self.OODS_CONSUME_QUEUE, self.on_message)
        self.oods_consumer.start()

        # messages from ArchiverController
        self.archive_consumer = Consumer(self.base_broker_url, self.parent, self.ARCHIVE_CTRL_PUBLISH_QUEUE,
                                         self.on_message)
        self.archive_consumer.start()

        # ack messages from Forwarder & ArchiveController
        self.forwarder_consumer = Consumer(self.base_broker_url,  self.parent, self.forwarder_publish_queue,
                                           self.on_message)
        self.forwarder_consumer.start()

        # telemetry messages from forwarder
        self.telemetry_consumer = Consumer(self.base_broker_url,  self.parent, self.TELEMETRY_QUEUE,
                                           self.on_telemetry)
        self.telemetry_consumer.start()

    async def stop_consumers(self):
        """ Stop all consumer connections
        """
        LOGGER.info("stopping consumers")
        if self.archive_consumer is not None:
            self.archive_consumer.stop()
        if self.forwarder_consumer is not None:
            self.forwarder_consumer.stop()
        if self.telemetry_consumer is not None:
            self.telemetry_consumer.stop()
        if self.oods_consumer is not None:
            self.oods_consumer.stop()

    def on_message(self, ch, method, properties, body):
        """ Route the message to the proper handler
        """
        msg_type = body['MSG_TYPE']
        if (msg_type != self.FWDR_HEALTH_CHECK_ACK) and (msg_type != 'ARCHIVE_HEALTH_CHECK_ACK'):
            LOGGER.info("received message")
            LOGGER.info(body)
        ch.basic_ack(method.delivery_tag)
        if msg_type in self._msg_actions:
            handler = self._msg_actions.get(msg_type)
            task = asyncio.create_task(handler(body))
        else:
            LOGGER.error(f"Unknown MSG_TYPE: {msg_type}")

    def on_telemetry(self, ch, method, properties, body):
        """ Called when telemetry is received.  This calls parent CSC object to emit the telemetry as a SAL message
        """
        LOGGER.info(f"message was: {body}")
        ch.basic_ack(method.delivery_tag)
        obsid = None
        raft = None
        sensor = None
        try:
            obsid, raft, sensor = self.parent.extract_filename_info(body['FILENAME'])
        except Exception as e:
            LOGGER.info(e)
        
        task = asyncio.create_task(self.parent.send_imageRetrievalForArchiving(self.CAMERA_NAME, obsid, raft, sensor, self.ARCHIVER_NAME))

    async def send_ingest_message_to_oods(self, body):
        msg = self.build_file_ingest_request_message(body)
        await self.publisher.publish_message(self.OODS_PUBLISH_QUEUE, msg)

    def build_file_ingest_request_message(self, msg):
        d = {}
        d['MSG_TYPE'] = self.FILE_INGEST_REQUEST
        d['CAMERA'] = self.CAMERA_NAME
        d['ARCHIVER'] = self.SHORT_NAME
        d['OBSID'] = msg['OBSID']
        d['FILENAME'] = msg['FILENAME']
        return d

    async def process_image_in_oods(self, msg):
        """ Handle image_in_oods message
        @param msg: contents of image_in_oods message
        """
        LOGGER.info(f"msg received: {msg}")
        camera = msg['CAMERA']
        obsid = msg['OBSID']
        archiver = msg['ARCHIVER']
        status_code = msg['STATUS_CODE']
        description = msg['DESCRIPTION']
        n_obsid = None
        raft = None
        sensor = None
        try:
            n_obsid, raft, sensor = self.parent.extract_filename_info(msg['FILENAME'])
        except Exception as e:
            LOGGER.info(e)

        task = asyncio.create_task(self.parent.send_imageInOODS(camera, n_obsid, raft, sensor, archiver, status_code, description))

    async def process_items_xferd_ack(self, msg):
        """ Handle at_items_xferd_ack message
        @param msg: contents of items_xferd_ack message
        """
        ack_id = msg["ACK_ID"]
        LOGGER.info(f"process_items_xferd: ack {ack_id} received")

    async def process_archiver_health_check_ack(self, msg):
        """ Handle archiver_health_check_ack message
        @param msg: contents of archiver_health_check_ack message
        """
        self.archive_heartbeat_evt.clear()

    async def publish_message(self, queue, msg):
        """ publish message
        @param queue: queue to write to
        @param msg: dict containing message contents
        """
        await self.publisher.publish_message(queue, msg)

    async def send_association_message(self):
        """ send an association message to inform the forwarder it has been picked
        """
        ack_id = await self.get_next_ack_id()
        msg = {}
        msg['ACK_ID'] = ack_id
        msg['MSG_TYPE'] = 'ASSOCIATED'
        msg['ASSOCIATION_KEY'] = self.ASSOCIATION_KEY
        msg['REPLY_QUEUE'] = self.forwarder_publish_queue
        await self.publish_message(self.forwarder_consume_queue, msg)

        code = 5752
        report = f"No association response from forwarder. Setting fault state with code = {code}"

        evt = await self.create_event(ack_id)
        waiter = Waiter(evt, self.parent, self.ack_timeout)
        self.startIntegration_ack_task = asyncio.create_task(waiter.pause(code, report))
        
    def send_telemetry(self, status_code, description):
        """ send telemetry
        @param status_code: message code
        @param description: message contents
        """
        msg = {}
        msg['MSG_TYPE'] = 'TELEMETRY'
        msg['DEVICE'] = self.DEVICE
        msg['STATUS_CODE'] = status_code
        msg['DESCRIPTION'] = description
        self.publish_message(self.TELEMETRY_QUEUE, msg)

    def build_archiver_message(self, ack_id, data):
        """ build a NEW_AT_ARCHIVE_ITEM message to send to the archiver
        @param ack_id: acknowledgment id to send
        @param data: CSC message contents to be used to send
        """
        LOGGER.info(f'data = {data}')
        d = {}

        d['MSG_TYPE'] = self.NEW_ARCHIVE_ITEM
        d['ACK_ID'] = ack_id
        d['JOB_NUM'] = self.get_next_jobnum()
        d['SESSION_ID'] = self.get_session_id()
        d['IMAGE_ID'] = data.imageName
        d['REPLY_QUEUE'] = self.forwarder_publish_queue

        d['imageName'] = data.imageName
        d['imageIndex'] = data.imageIndex
        #d['imageSequenceName'] = data.imageSequenceName
        d['imagesInSequence'] = data.imagesInSequence
        d['imageDate'] = data.imageDate
        d['exposureTime'] = data.exposureTime
        return d

    def build_startIntegration_message(self, ack_id, data):
        """ build a startIntegration message to send to the Forwarder
        @param ack_id: acknowledgment id to send
        @param data: CSC message contents to be used to send
        """
        d = {}
        d['MSG_TYPE'] = self.FWDR_XFER_PARAMS
        d['SESSION_ID'] = self.get_session_id()
        d['IMAGE_ID'] = data['IMAGE_ID']
        d['DEVICE'] = 'AT'
        d['JOB_NUM'] = self.get_jobnum()
        d['ACK_ID'] = ack_id
        d['REPLY_QUEUE'] = self.forwarder_publish_queue
        targetDir = data['TARGET_DIR']
        location = f"{self.archive_login}@{self.archive_ip}:{targetDir}"
        d['TARGET_LOCATION'] = location

        xfer_params = {}
        xfer_params['RAFT_LIST'] = self.wfs_raft
        xfer_params['RAFT_CCD_LIST'] = self.wfs_ccd
        xfer_params['AT_FWDR'] = self.forwarder_host  # self._current_fwdr['FQN']

        d['XFER_PARAMS'] = xfer_params
        return d

    def build_endReadout_message(self, ack_id, data):
        """ build an endReadout message to send to the Forwarder
        @param ack_id: acknowledgment id to send
        @param data: CSC message contents to be used to send
        """
        d = {}
        d['MSG_TYPE'] = self.FWDR_END_READOUT
        d['JOB_NUM'] = self.get_jobnum()
        d['SESSION_ID'] = self.get_session_id()
        d['IMAGE_ID'] = data.imageName
        d['ACK_ID'] = ack_id
        d['REPLY_QUEUE'] = self.forwarder_publish_queue
        d['IMAGES_IN_SEQUENCE'] = data.imagesInSequence
        d['IMAGE_INDEX'] = data.imageIndex
        return d

    def build_largeFileObjectAvailable_message(self, ack_id, data):
        """ build a largeFileObjectAvailable message to send to the Forwarder
        @param ack_id: acknowledgment id to send
        @param data: CSC message contents to be used to send
        """
        d = {}
        d['MSG_TYPE'] = self.FWDR_HEADER_READY
        d['FILENAME'] = data.url
        d['IMAGE_ID'] = data.id
        d['ACK_ID'] = ack_id
        d['REPLY_QUEUE'] = self.forwarder_publish_queue
        return d

    async def process_association_ack(self, msg):
        """ Handle incoming association_ack message
        @param msg: contents of association ack
        """
        # TODO: this is temporary until the Forwarder returns ACK_ID; after that starts happening, this code can be removed
        if "ACK_ID" in msg:
            ack_id = msg["ACK_ID"]
            LOGGER.info(f"association ack received {ack_id}")
            evt = await self.clear_event(ack_id)
            if evt is None:
                LOGGER.info(f"Association ACK {ack_id} is unknown.  Ignored.")
                return
        else:
            LOGGER.info("Missing ACK_ID in association message.")
            return
        
        watcher = Watcher(self.stop_watcher_evt, self.parent, self.scoreboard)
        watcher_task = asyncio.create_task(watcher.peek(msg['ASSOCIATION_KEY'], self.seconds_to_expire))


    async def process_new_item_ack(self, msg):
        """ Handle incoming new_at_item_ack message
        @param msg: contents of new_at_item_ack
        """
        ack_id = msg["ACK_ID"]
        LOGGER.info(f"process_new_item_ack ack_id = {ack_id} received")
        evt = await self.clear_event(ack_id)
        # this is scheduled, since process_new_item_ack is never await-ed
        task = asyncio.create_task(self.send_startIntegration(msg))

    async def send_startIntegration(self, data):
        """ send the startIntegration message
        @param data: contents of CSC startIntegration message
        """
        ack_id = await self.get_next_ack_id()
        msg = self.build_startIntegration_message(ack_id, data)

        await self.publish_message(self.forwarder_consume_queue, msg)
        LOGGER.info("startIntegration sent to forwarder")

        if self.wait_for_ack_timeouts:
            code = 5752
            report = f"No xfer_params response from forwarder. Setting fault state with code = {code}"

            # creates a timer waiting for an acknowledgement
            evt = await self.create_event(ack_id)
            waiter = Waiter(evt, self.parent, self.ack_timeout)
            task = asyncio.create_task(waiter.pause(code, report))

    #
    # startIntegration
    #
    async def transmit_startIntegration(self, data):
        """transmit startIntegration to the forwarder
        @param data: the contents of the startIntegration CSC message
        """
        ack_id = await self.get_next_ack_id()

        # first we send a message to the archiver, to obtain the correct target directory
        msg = self.build_archiver_message(ack_id, data)
        LOGGER.info(f"transmit_startIntegration msg = {msg}")

        await self.publish_message(self.ARCHIVE_CTRL_CONSUME_QUEUE, msg)

        # now we set up a wait for the ack. If the ack doesn't appear within the time 
        # frame allotted, a fault is thrown.  Otherwise, when the ack message is received,
        # the data is extracted within the "process_new_item_ack" method, and the
        # "startIntegration" message is build and sent to the forwarder from that method

        evt = await self.create_event(ack_id)
        if self.wait_for_ack_timeouts:
            code = 5752
            report = f"No ack response from at archive controller"

            waiter = Waiter(evt, self.parent, self.ack_timeout)
            task = asyncio.create_task(waiter.pause(code, report))

    async def process_xfer_params_ack(self, msg):
        """ Handle xfer_params_ack message
        """
        ack_id = msg["ACK_ID"]
        LOGGER.info(f"startIntegration ack_id = {ack_id} received")
        evt = await self.clear_event(ack_id)
        pass

    #
    # endReadout
    #
    async def transmit_endReadout(self, data):
        """transmit endReadout to the forwarder
        @param data: the contents of the endReadout CSC message
        """
        ack_id = await self.get_next_ack_id()

        msg = self.build_endReadout_message(ack_id, data)
        LOGGER.info(f"transmit_endReadout msg = {msg}")
        await self.publish_message(self.forwarder_consume_queue, msg)

        evt = await self.create_event(ack_id)
        if self.wait_for_ack_timeouts:
            code = 5753
            report = f"No endReadout ack from forwarder. Setting fault state with code = {code}"
            waiter = Waiter(evt, self.parent, self.ack_timeout)
            task = asyncio.create_task(waiter.pause(code, report))

    async def process_fwdr_end_readout_ack(self, msg):
        """ Handle at_fwder_end_readout_ack message
        """
        ack_id = msg["ACK_ID"]
        LOGGER.info(f"endReadout ack_id = {ack_id} received")
        evt = await self.clear_event(ack_id)
        pass

    #
    # largeFileObjectAvailable
    #
    async def transmit_largeFileObjectAvailable(self, data):
        """transmit largeFileObjectAvailable to the forwarder
        @param data: the contents of the largeFileObjectAvailable CSC message
        """
        ack_id = await self.get_next_ack_id()
        msg = self.build_largeFileObjectAvailable_message(ack_id, data)
        LOGGER.info(f"transmit_largeFileObjectAvailable msg = {msg}")
        await self.publish_message(self.forwarder_consume_queue, msg)

        evt = await self.create_event(ack_id)
        if self.wait_for_ack_timeouts:
            code = 5754
            report = f"No largeFileObjectAvailable ack from forwarder. Setting fault state with code = {code}"
            waiter = Waiter(evt, self.parent, self.ack_timeout)
            task = asyncio.create_task(waiter.pause(code, report))

    async def process_header_ready_ack(self, msg):
        """ Handle header_ready_ack message
        """
        ack_id = msg["ACK_ID"]
        LOGGER.info(f"largeFileObjectAvailable ack_id = {ack_id} received")
        evt = await self.clear_event(ack_id)
        pass

    #
    # Heartbeat
    #
    async def emit_heartbeat(self, component_name, queue, msg_type, heartbeat_event):
        try:
            LOGGER.info(f"starting heartbeat with {component_name} on {queue}")

            pub = Publisher(self.base_broker_url, csc_parent=self.parent, logger_level=LOGGER.debug)
            await pub.start()

            while True:
                ack_id = await self.get_next_ack_id()
                msg = {"MSG_TYPE": msg_type,
                       "ACK_ID": ack_id,
                       "SESSION_ID": self.get_session_id(),
                       "REPLY_QUEUE": self.forwarder_publish_queue}
                LOGGER.debug(f"about to send {msg}")
                await pub.publish_message(queue, msg)

                code=5751
                report=f"failed to received heartbeat ack from {component_name}"

                waiter = Waiter(heartbeat_event, self.parent, self.ack_timeout)
                heartbeat_task = asyncio.create_task(waiter.pause(code, report))
                await heartbeat_task
                if heartbeat_event.is_set():
                    await pub.stop()
                    return
        except asyncio.CancelledError:
            await pub.stop()
        except Exception as e:
            await pub.stop()
            self.parent.call_fault(code=5751, report=f"failed to publish message to {queue}")
            return
