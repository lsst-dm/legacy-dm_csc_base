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

import datetime
import logging
from lsst.ctrl.iip.base import base

LOGGER = logging.getLogger(__name__)


class Director(base):

    def __init__(self, config_filename, log_filename):
        super().__init__(config_filename, log_filename)

        self.initialize_session()

        cdm = self.getConfiguration()
        root = cdm["ROOT"]
        self.base_broker_addr = root["BASE_BROKER_ADDR"]

        cred = self.getCredentials()

        service_user = cred.getUser('service_user')
        service_passwd = cred.getPasswd('service_passwd')

        url = f"amqp://{service_user}:{service_passwd}@{self.base_broker_addr}"

        self.base_broker_url = url

    def initialize_session(self):
        self.session_id = str(datetime.datetime.now())
        self.jobnum = 0

    def get_session_id(self):
        return self.session_id

    def get_jobnum(self):
        return self.jobnum

    def get_next_jobnum(self):
        self.jobnum += 1
        return self.jobnum
