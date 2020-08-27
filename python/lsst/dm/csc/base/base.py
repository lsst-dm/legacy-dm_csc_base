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
from logging.handlers import RotatingFileHandler
import os
import os.path
import sys
import time
import yaml
from lsst.dm.csc.base.Credentials import Credentials

LOGGER = logging.getLogger(__name__)


class Base:
    """Base class which sets up logging, configuration and credentials

    Parameters
    ----------
        name : `str`
            name of device
        config_filename : `str`
            YAML configuration file name
        log_filename : `str`
            file name to which logging messages will be sent
    """

    def __init__(self, name, config_filename, log_filename):
        self._name = name
        self._config = self.loadConfigFile(config_filename)
        self.setupLogging(log_filename)
        self._cred = Credentials('iip_cred.yaml')

    def getName(self):
        """Get the device name

        Returns
        -------
        The name of this service
        """
        return self._name

    def loadConfigFile(self, filename):
        """Load configuration file from configuration directory.  The
        default location is $CTRL_IIP_DIR/etc/config.  If the environment
        variable $IIP_CONFIG_DIR exists, files are loaded from that location
        instead.

        Parameters
        ----------
        filename : `str`
            The YAML configuration filename

        Returns
        -------
        A dict containing the configuration file information
        """

        config_file = filename

        if "IIP_CONFIG_DIR" in os.environ:
            config_dir = os.environ["IIP_CONFIG_DIR"]
            config_file = os.path.join(config_dir, config_file)
        else:
            work_dir = f"{self._name}_DIR"
            if work_dir in os.environ:
                config_dir = os.environ[work_dir]
                config_dir = os.path.join(config_dir, "etc", "config")
                config_file = os.path.join(config_dir, config_file)
            else:
                raise Exception(f"environment variable {work_dir} not defined")

        print(f"loading {config_file}")
        try:
            f = open(config_file)
        except Exception:
            msg = f"Can't open {config_file}"
            print(msg)
            raise FileNotFoundError(msg)

        config = None
        try:
            config = yaml.safe_load(f)
        except Exception:
            print("Error reading %s" % config_file)
        finally:
            f.close()
        return config

    def getCredentials(self):
        """Get the credential information

        Returns
        -------
        A dict containing credential information
        """
        return self._cred

    def getConfiguration(self):
        """Get the configuration information

        Returns
        -------
        A dict containing configuration information
        """
        return self._config

    def setupLogging(self, filename):
        """Setup writing to a log. If the IIP_LOG_DIR environment variable
        is set, use that.  Otherwise, use log_dir_location if it was
        specified. If it wasn't, default to /tmp.

        Parameters
        -----------
        filename : `str`
            the log file to write to
        """

        log_dir_location = self._config['ROOT'].get('LOGGING_DIR', None)

        log_dir = None

        if "IIP_LOG_DIR" in os.environ:
            log_dir = os.environ["IIP_LOG_DIR"]
        else:
            if log_dir_location is not None:
                log_dir = log_dir_location
            else:
                log_dir = None

        f = '%(levelname) -10s %(asctime)s.%(msecs)03dZ %(name) -30s %(funcName) '
        f = f + '-35s %(lineno) -5d: %(message)s'
        FORMAT = (f)
        LOGGER = logging.getLogger(__name__)
        logging.Formatter.converter = time.gmtime
        LOGGER.setLevel(logging.DEBUG)

        if log_dir is None:
            # if we're here, there was no LOGGING_DIR entry in the config file,
            # and IIP_LOG_DIR hasn't been set.  Therefore, write to stdout.
            handler = logging.StreamHandler(sys.stdout)
            LOGGER.addHandler(handler)
            logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
        else:
            # if we're here, either LOGGING_DIR was set, or IIP_LOG_DIR was set, so write files to
            # the directory that was indicated.
            log_file = os.path.join(log_dir, filename)
            handler = RotatingFileHandler(log_file, maxBytes=2000000, backupCount=10)
            LOGGER.addHandler(handler)
            logging.basicConfig(filename=log_file, level=logging.INFO, format=FORMAT,
                                datefmt="%Y-%m-%d %H:%M:%S")

    def shutdown(self):
        """Shutdown all services
        """
        pass
