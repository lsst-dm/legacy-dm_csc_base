#!/bin/bash
if [ -z ${DM_CSC_BASE_RELEASE} ]; then
	echo "environment variable DM_CSC_BASE_RELEASE has to be set"
	exit 1
fi
if [ -z ${DM_CSC_BASE_DIR} ]; then
	echo "environment variable DM_CSC_BASE_DIR has to be set"
	exit 1
fi
HERE=$PWD
PID=$$
echo $PID
MY_BUILD_DIR=/tmp/build.$PID
mkdir -p $MY_BUILD_DIR
cd $MY_BUILD_DIR
mkdir -p BUILD BUILDROOT RPMS SOURCES SPECS  SRPMS
cp $DM_CSC_BASE_DIR/etc/specs/dm_csc_base.spec SPECS
cd SOURCES
wget https://github.com/lsst-dm/dm_csc_base/archive/${DM_CSC_BASE_RELEASE}.zip
cd ..
rpmbuild --define "_topdir `pwd`" --target noarch -ba SPECS/*spec
cp $MY_BUILD_DIR/RPMS/noarch/*rpm $DM_CSC_BASE_DIR/etc
cd $HERE
