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
import asynctest

from lsst.dm.csc.base.scoreboard import Scoreboard


class ScoreboardTestCase(asynctest.TestCase):

    def test_scoreboard(self):
        sb = Scoreboard("AT", 1, "localhost")

        sb.set_session("test_session")
        s = sb.get_session()
        self.assertEqual(s, "test_session")

        sb.set_state("test_state")
        s = sb.get_state()
        self.assertEqual(s, "test_state")
