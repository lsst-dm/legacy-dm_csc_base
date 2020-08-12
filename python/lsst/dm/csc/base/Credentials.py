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
import os
import os.path
import stat
import sys
import yaml

LOGGER = logging.getLogger(__name__)


class Credentials:
    """Represents credential information for RabbitMQ authentication

    Parameters
    ----------
    filename : `str`
        The YAML file name of the file containing credential information
    """

    def __init__(self, filename):
        self.cred = self.loadSecureFile(filename)

    def getUser(self, user_alias):
        """Get the user name of the RabbitMQ user
        Parameters
        ----------
        user_alias : `str`
            user alias

        Returns
        -------
        The RabbitMQ user name
        """
        return self.cred['rabbitmq_users'][user_alias]

    def getPasswd(self, passwd_alias):
        """Get the password of the RabbitMQ user
        Parameters
        ----------
        password_alias : `str`
            password alias

        Returns
        -------
        The RabbitMQ password
        """
        return self.cred['rabbitmq_users'][passwd_alias]

    def loadSecureFile(self, cred_file):
        """Load a secure YAML file. The file must be in a secure directory

        Returns
        -------
        A dictionary containing creditial information
        """

        if "IIP_CREDENTIAL_DIR" in os.environ:
            config_dir = os.environ["IIP_CREDENTIAL_DIR"]
        else:
            home = os.environ["HOME"]
            config_dir = os.path.join(home, ".lsst")
        # check for the existence of the configuration directory
        if os.path.isdir(config_dir):
            stat_info = os.stat(config_dir)
            mode = stat_info.st_mode

            # check that the permissions are set to rwx for only the user
            if (mode & (stat.S_IWOTH | stat.S_IWGRP | stat.S_IROTH | stat.S_IRGRP)):
                msg = "directory '%s' is unsecure. Run 'chmod 700 %s'." % (config_dir, config_dir)
                print(msg)
                LOGGER.info(msg)
                raise PermissionError(msg)
            filename = os.path.join(config_dir, cred_file)
            # check that the credential file exists
            if os.path.isfile(filename):
                stat_info = os.stat(filename)
                mode = stat_info.st_mode
                # check that the credential file is set to rw for only the user
                if (mode & (stat.S_IWOTH | stat.S_IWGRP | stat.S_IROTH | stat.S_IRGRP)):
                    msg = "file '%s' is unsecure.  Run 'chmod 600 %s'." % (filename, filename)
                    print(msg)
                    LOGGER.info(msg)
                    raise PermissionError(msg)
                else:
                    # after all that checking, load the YAML file containing the secure file
                    return self.loadYamlFile(filename)
            else:
                msg = "can not find credentials file '%s'." % filename
                LOGGER.info(msg)
                raise FileNotFoundError(msg)
        else:
            path = os.path.join(config_dir, cred_file)
            msg = "can not read credentials file '%s'." % path
            print(msg)
            LOGGER.info(msg)
            raise PermissionError(msg)

    def loadYamlFile(self, config_file):
        try:
            f = open(config_file)
        except Exception:
            msg = "Can't open %s" % config_file
            print(msg)
            LOGGER.info(msg)
            raise PermissionError(msg)

        config = None
        try:
            config = yaml.safe_load(f)
        except Exception:
            msg = "Error reading %s" % config_file
            print(msg)
            LOGGER.info(msg)
        finally:
            f.close()
        return config
