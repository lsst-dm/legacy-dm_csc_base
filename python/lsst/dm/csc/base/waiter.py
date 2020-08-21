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

LOGGER = logging.getLogger(__name__)


class Waiter:
    """Waiter is use to briefly pause while other work is going on.  If
    a fault occurs during this time, report the error through the archiver_csc

    Parameters
    ----------
    evt : `asyncio.Event`
        flag to indicate fault
    parent: `lsst.dm.csc.base.archiver_csc`
        the calling archiver_csc
    timeout: `int`
        time in seconds to pause
    """

    def __init__(self, evt, parent, timeout):
        self.evt = evt
        self.evt.set()
        self.parent = parent
        self.timeout = timeout

    async def pause(self, code, report):
        """Sleep for a short time; if a failure occurred during this time,
        report it

        Parameters
        ----------
        code : `int`
            fault code
        report : `str`
            Description of what happened in this fault
        """
        await asyncio.sleep(self.timeout)
        if self.evt.is_set():
            self.parent.call_fault(code=code, report=report)
