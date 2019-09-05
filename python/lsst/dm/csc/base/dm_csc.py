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
import traceback
from lsst.ts.salobj import State
from lsst.ts.salobj import ConfigurableCsc

LOGGER = logging.getLogger(__name__)


class dm_csc(ConfigurableCsc):
    def __init__(self, name, index, schema_path, config_dir, initial_state, initial_simulation_mode):
        super().__init__(name, index=index, schema_path=schema_path, config_dir=config_dir,
                         initial_state=initial_state, initial_simulation_mode=initial_simulation_mode)
        self.scoreboard = None
        self.state_to_str = {
            State.DISABLED: "disabled",
            State.ENABLED: "enabled",
            State.FAULT: "fault",
            State.OFFLINE: "offline",
            State.STANDBY: "standby"}


    @staticmethod
    def get_config_pkg():
        return "dm_config_at"

    async def configure(self, config):
        """Configure this CSC and output the ``settingsApplied`` event.

        Parameters
        ----------
        config : `types.SimpleNamespace`
            Configuration, as described by ``schema/ATDomeTrajectory.yaml``
        """
        self.config = config
        LOGGER.info("configuring")
        self.evt_settingsApplied.set_put(
            settings="normal",
            tsXMLVersion=self.config.tsXMLVersion,
            tsSALVersion=self.config.tsSALVersion,
            l1dmRepoTag=self.config.l1dmRepoTag
        )

    def report_summary_state(self):
        super().report_summary_state()
        state = self.state_to_str[self.summary_state]
        if self.scoreboard is not None:
            self.scoreboard.set_state(state)
