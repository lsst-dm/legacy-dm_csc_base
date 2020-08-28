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

import logging
from lsst.dm.csc.base.dm_csc import DmCSC
from lsst.ts import salobj

LOGGER = logging.getLogger(__name__)


class ArchiverCSC(DmCSC):
    """Base class used for Archiver Commandable SAL Components (CSC)

    Parameters
    ----------
    name : `str`
        Name of SAL component.
    index : `int` or `None`
        SAL component index, or 0 or None if the component is not indexed.
    initial_state : `State` or `int`, optional
        The initial state of the CSC. This is provided for unit testing,
        as real CSCs should start up in `State.STANDBY`, the default.

    """

    def __init__(self, name, index, initial_state=salobj.State.STANDBY):
        super().__init__(name, index=index, initial_state=initial_state)

    async def send_imageRetrievalForArchiving(self, camera, archiverName, info):
        """Send SAL message indicating that an image has been retrieved for archiving
    
        Parameters
        ----------
        camera : `str`
            Name of the camera from which the image came
        archiverName : `str`
            Name of this archiver
        info : `dict`
            information about the image
        """
        obsid = info['OBSID']
        raft = "undef"
        if 'RAFT' in info:
            raft = info['RAFT']
        sensor = "undef"
        if 'SENSOR' in info:
            sensor = info['SENSOR']
        statusCode = info['STATUS_CODE']
        description = info['DESCRIPTION']
        s = f'sending camera={camera} obsid={obsid} raft={raft} sensor={sensor}  '
        s = s + f'archiverName={archiverName}, statusCode={statusCode}, description={description}'
        LOGGER.info(s)
        self.evt_imageRetrievalForArchiving.set_put(camera=camera, obsid=obsid, raft=raft,
                                                    sensor=sensor, archiverName=archiverName,
                                                    statusCode=statusCode, description=description)

    async def send_imageInOODS(self, info):
        """Send SAL message that the images has been ingested into the OODS

        Parameters
        ----------
        info : `dict`
            information about the image
        """
        camera = info['CAMERA']
        archiverName = info['ARCHIVER']
        obsid = info['OBSID']
        raft = "undef"
        if 'RAFT' in info:
            raft = info['RAFT']
        sensor = "undef"
        if 'SENSOR' in info:
            sensor = info['SENSOR']
        statusCode = info['STATUS_CODE']
        description = info['DESCRIPTION']

        s = f'sending camera={camera} obsid={obsid} raft={raft} sensor={sensor} '
        s = s + f'archiverName={archiverName}, statusCode={statusCode}, description={description}'
        LOGGER.info(s)
        self.evt_imageInOODS.set_put(camera=camera,
                                     obsid=obsid,
                                     raft=raft,
                                     sensor=sensor,
                                     archiverName=archiverName,
                                     statusCode=statusCode,
                                     description=description)

    async def start_services(self):
        """Start all support services
        """
        await self.director.start_services()

    async def stop_services(self):
        """Stop all support services
        """
        await self.director.stop_services()

    async def do_resetFromFault(self, data):
        """resetFromFault. Required by ts_salobj csc
        """
        print(f"do_resetFromFault called: {data}")

    async def startIntegrationCallback(self, data):
        """Send the startIntegration message to the Forwarder
        """
        self.assert_enabled("startIntegration")
        LOGGER.info("startIntegration callback")
        # message actually sent by the director
        await self.director.transmit_startIntegration(data)

    async def endReadoutCallback(self, data):
        """Send the endReadout message to the Forwarder
        """
        self.assert_enabled("endReadout")
        LOGGER.info("endReadout")
        # message actually sent by the director
        await self.director.transmit_endReadout(data)

    async def largeFileObjectAvailableCallback(self, data):
        """Send the largeFileObjectAvailable  message to the Forwarder
        """
        self.assert_enabled("largeFileObjectAvailable")
        LOGGER.info("largeFileObjectAvailable")
        # message actually sent by the director
        await self.director.transmit_largeFileObjectAvailable(data)
