#!/usr/bin/python3
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

import argparse
import asyncio
import os
import sys
from lsst.ts import salobj


class Commander:

    def __init__(self, device_name, command, timeout, settings):
        self.device_name = device_name
        self.command = command
        self.timeout = timeout
        self.settings = settings

    async def run_command(self):
        async with salobj.Domain() as domain:
            arc = salobj.Remote(domain=domain, name=self.device_name, index=0)
            await arc.start_task

            try:
                if self.command == "start":
                    await arc.cmd_start.set_start(settingsToApply=self.settings, timeout=self.timeout)
                else:
                    cmd = getattr(arc, f"cmd_{self.command}")
                    await cmd.set_start(timeout=self.timeout)
            except Exception as e:
                print(e)


if __name__ == "__main__":

    name = os.path.basename(sys.argv[0])
    parser = argparse.ArgumentParser(prog=name, description="Send SAL commands to devices")
    parser.add_argument("-D", "--device", type=str, dest="device", required=True,
                        help="component to which the command will be sent")
    parser.add_argument("-t", "--timeout", type=int, dest="timeout", default=5, help="command timeout")

    subparsers = parser.add_subparsers(dest="command")

    start_parser = subparsers.add_parser('start')
    start_parser.add_argument('-s', '--settings', dest="settings", required=True, help="setting to apply")

    cmds = ['enable', 'disable', 'enterControl', 'exitControl', 'standby', 'abort', 'resetFromFault']
    for x in cmds:
        p = subparsers.add_parser(x)

    args = parser.parse_args()

    settings = None
    if args.command == "start":
        settings = args.settings

    cmdr = Commander(args.device, args.command, args.timeout, settings)
    asyncio.run(cmdr.run_command())
