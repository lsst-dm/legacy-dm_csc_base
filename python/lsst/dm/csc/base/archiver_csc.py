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
import pathlib
from lsst.dm.csc.base.dm_csc import dm_csc
from lsst.ts import salobj
from lsst.ts.salobj import State

LOGGER = logging.getLogger(__name__)


class ArchiverCSC(dm_csc):

    def __init__(self, name, index, schema_path=None, config_dir=None,
                 initial_state=salobj.State.STANDBY, initial_simulation_mode=0):
        super().__init__(name, index=index, schema_path=schema_path, config_dir=config_dir,
                         initial_state=initial_state, initial_simulation_mode=initial_simulation_mode)

    async def send_imageRetrievalForArchiving(self, camera, obsid, archiverName):
        LOGGER.info(f"sending camera={camera}; obsid={obsid}; archiverName={archiverName}")
        self.evt_imageRetrievalForArchiving.set_put(camera=camera, obsid=obsid, archiverName=archiverName)

    async def send_imageInOODS(self, camera, obsid, archiverName, statusCode, description):
        LOGGER.info(f"sending {camera} {obsid} {archiverName} {statusCode}: {description}")

        self.evt_imageInOODS.set_put(camera=camera,
                                     obsid=obsid,
                                     archiverName=archiverName,
                                     statusCode=statusCode,
                                     description=description)

    def report_summary_state(self):
        super().report_summary_state()

        s_cur = None
        if self.current_state is not None:
            s_cur = self.state_to_str[self.current_state]
        s_sum = self.state_to_str[self.summary_state]

        LOGGER.info(f"current state is: {s_cur}; transition to {s_sum}")

        # Services are started when transitioning from STANDBY to DISABLED.  Services
        # are stopped when transitioning from DISABLED to STANDBY.   Services are always stopped
        # when moving from any state to FAULT. The internal state machine in the base class only
        # allows transition from FAULT to STANDBY.

        # NOTE: The following can be optimized, but hasn't been to give a clearer picture
        #       about which state transitions occur, and what happens when they do occur.

        # if current_state hasn't been set, and the summary_state is STANDBY, we're just
        # starting up, so don't do anything but set the current state to STANBY
        if (self.current_state is None) and (self.summary_state == State.STANDBY):
            self.current_state = State.STANDBY
            self.transitioning_to_fault_evt.clear()
            return

        # if going from STANDBY to DISABLED, start external services
        if (self.current_state == State.STANDBY) and (self.summary_state == State.DISABLED):
            asyncio.ensure_future(self.start_services())
            self.current_state = State.DISABLED
            return

        # if going from DISABLED to STANDBY, stop external services
        if (self.current_state == State.DISABLED) and (self.summary_state == State.STANDBY):
            asyncio.ensure_future(self.stop_services())
            self.current_state = State.STANDBY
            return


        # if going from STANDBY to FAULT, kill any external services that started
        if (self.current_state == State.STANDBY) and (self.summary_state == State.FAULT):
            asyncio.ensure_future(self.stop_services())
            self.current_state = State.FAULT
            return

        # if going from ENABLED to FAULT, don't do anything, so we can debug
        if (self.current_state == State.ENABLED) and (self.summary_state == State.FAULT):
            asyncio.ensure_future(self.stop_services())
            self.current_state = State.FAULT
            return

        # if going from DISABLED to FAULT, don't do anything, so we can debug
        if (self.current_state == State.DISABLED) and (self.summary_state == State.FAULT):
            asyncio.ensure_future(self.stop_services())
            self.current_state = State.FAULT
            return

        # if going from FAULT to STANDBY, services have already been stopped
        if (self.current_state == State.FAULT) and (self.summary_state == State.STANDBY):
            self.current_state = State.STANDBY
            self.transitioning_to_fault_evt.clear()
            return

        # These are all place holders and only update state

        # if going from DISABLED to ENABLED leave external services alone, but accept control commands
        if (self.current_state == State.DISABLED) and (self.summary_state == State.ENABLED):
            self.current_state = State.ENABLED
            return

        # if going from ENABLED to DISABLED, leave external services alone, but reject control commands
        if (self.current_state == State.ENABLED) and (self.summary_state == State.DISABLED):
            self.current_state = State.DISABLED
            return

    def call_fault(self, code, report):
        if self.transitioning_to_fault_evt.is_set():
            return
        self.transitioning_to_fault_evt.set()
        LOGGER.info(report)
        self.fault(code, report)

    async def start_services(self):
        await self.director.start_services()

    async def stop_services(self):
        await self.director.stop_services()

    async def do_resetFromFault(self, data):
        print("do_resetFromFault called")
        print(data)

    async def startIntegrationCallback(self, data):
        self.assert_enabled("startIntegration")
        LOGGER.info("startIntegration callback")
        await self.director.transmit_startIntegration(data)

    async def endReadoutCallback(self, data):
        self.assert_enabled("endReadout")
        LOGGER.info("endReadout")
        await self.director.transmit_endReadout(data)

    async def largeFileObjectAvailableCallback(self, data):
        self.assert_enabled("largeFileObjectAvailable")
        LOGGER.info("largeFileObjectAvailable")
        await self.director.transmit_largeFileObjectAvailable(data)