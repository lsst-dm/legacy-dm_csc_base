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

    async def send_imageRetrievalForArchiving(self, camera, archiverName, dictionary):
        obsid = dictionary['OBSID']
        raft = "undef"
        if 'RAFT' in dictionary:
            raft = dictionary['RAFT']
        sensor = "undef"
        if 'SENSOR' in dictionary:
            sensor = dictionary['SENSOR']
        statusCode = dictionary['STATUS_CODE']
        description = dictionary['DESCRIPTION']
        LOGGER.info(f"sending camera={camera} obsid={obsid} raft={raft} sensor={sensor}  archiverName={archiverName}, statusCode={statusCode}, description={description}")
        self.evt_imageRetrievalForArchiving.set_put(camera=camera, obsid=obsid, raft=raft, sensor=sensor, archiverName=archiverName, statusCode=statusCode, description=description)

    async def send_imageInOODS(self, dictionary):

        camera = dictionary['CAMERA']
        archiverName = dictionary['ARCHIVER')
        obsid = dictionary['OBSID']
        raft = dictionary['RAFT']
        sensor = dictionary['SENSOR']
        statusCode = dictionary['STATUS_CODE']
        description = dictionary['DESCRIPTION']

        LOGGER.info(f"sending camera={camera} obsid={obsid} raft={raft} sensor={sensor}  archiverName={archiverName}, statusCode={statusCode}, description={description}")
        self.evt_imageInOODS.set_put(camera=camera,
                                     obsid=obsid,
                                     raft=raft,
                                     sensor=sensor,
                                     archiverName=archiverName,
                                     statusCode=statusCode,
                                     description=description)

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
