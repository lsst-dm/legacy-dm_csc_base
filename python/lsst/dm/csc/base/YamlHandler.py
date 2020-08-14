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


from copy import deepcopy
import yaml
import datetime


class YamlHandler:
    """Handler for YAML data
    Parameters

    ----------
    callback : `Method`
        method to call when message is decoded
    """
    def __init__(self, callback=None):
        self._consumer_callback = callback
        yaml.add_representer(datetime.time, self.dt_representer, Dumper=yaml.SafeDumper)
        yaml.add_constructor('datetime.time', self.dt_constructor, Loader=yaml.SafeLoader)
        self.dateformat = "%Y:%m:%dT%H%M%S.%f"

    def dt_constructor(self, loader, node):
        """A datetime constructor for incoming YAML data

        Parameters
        ----------
        loader: `yaml.Loader`
            loader to use to import data from a ScalarNode
        node : `yaml.ScalarNode`
            A YAML ScalarNode
        """
        data = loader.construct_scalar(node)
        return datetime.datetime.strptime(data, self.dateformat)

    def dt_representer(self, dumper, data):
        """Date time representation of current datetime

        Parameters
        ----------
        dumper: yaml.Dumper
            Use to create YAML representation of datetime
        data: `str`
            unused
        """
        s = data.strftime(self.dateformat)
        return dumper.represent_scalar('datetime.time', s)

    def yaml_callback(self, ch, method, properties, body):
        """Decode the message body before consuming Setting the consumer callback function

        Parameters
        ----------
        ch : `Channel`
        method : `Method`
        properties : `Properties`
        pydict : `dict`
        """
        pydict = self.decode_message(body)
        self._consumer_callback(ch, method, properties, pydict)

    def encode_message(self, dictValue):
        """encode a dictionary as YAML

        Parameters
        ----------
        dictValue : `dict`
            Dictionary containing information to be encoded into YAML

        Returns
        -------
        YAML string containing representation of dictValue
        """
        pydict = deepcopy(dictValue)
        yaml_body = yaml.safe_dump(pydict)
        return yaml_body

    def decode_message(self, body):
        """decode YAML into a dictionary

        Parameters
        ----------
        body : `str`
            YAML string

        Returns
        -------
        A dict representation of the YAML string
        """
        tmpdict = yaml.safe_load(body)
        return tmpdict
