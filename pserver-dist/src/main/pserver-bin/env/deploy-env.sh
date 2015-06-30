#!/bin/bash
########################################################################################################################
# 
#  Copyright (C) 2010-2013 by the pserver project
# 
#  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
#  the License. You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
#  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
#  specific language governing permissions and limitations under the License.
# 
########################################################################################################################

# set root dir if not done yet
if [ -z ${PSERVER_ROOT_DIR+x} ] || [ -z "${PSERVER_ROOT_DIR}" ]; then
	PWD=$(dirname "$0"); PWD=$(cd "${PWD}"; pwd);
	PSERVER_ROOT_DIR=$(cd "${PWD}/../.."; pwd)
fi

function process_logs() {
    for f in "${PSERVER_LOG_DIR_TMP}/"*; do
    	if [ -f "${f}" ]; then
	        df="${PSERVER_LOG_DIR}/$(basename ${f})"
	        # if the destination file does not exist, we can just copy it
	        if [ ! -f "${df}" ]; then
	            mv "${f}" "${df}"
	        # else we need to append the diff
	        else
	            f_lc=$(cat "${f}" | wc -l)
	            df_lc=$(cat "${df}" | wc -l)
	            if [ ${f_lc} -gt ${df_lc} ]; then
	                tail -n $((f_lc-df_lc)) ${f} >> "${df}"
	            fi
	        fi
        fi
    done
}

########################################################################################################################
# DEFAULT CONFIG VALUES: These values will be used when nothing has been specified in tools/wally.conf
# -or- the respective context variables are not set.
########################################################################################################################


# WARNING !!! , these values are only used if there is nothing else is specified in
# tools/deploy.conf
#

DEFAULT_PSERVER_DESTINATION_DIRECTORY="/data/$(whoami)/pserver"    # directory on wally nodes the pserver binaries will be copied into
DEFAULT_PSERVER_STAGING_DIRECTORY="${DEFAULT_PSERVER_DESTINATION_DIRECTORY}"
DEFAULT_PSERVER_STAGING_HOST="localhost"
DEFAULT_ZOOKEEPER_INSTALL_DIR="/data/$(whoami)/zookeeper"
DEFAULT_ZOOKEEPER_DATA_DIR="/data/$(whoami)/zookeeper/data"
DEFAULT_PSERVER_CONFIG_FILE="pserver.conf"
DEFAULT_ZOOKEEPER_DIST_URL="http://archive.apache.org/dist/zookeeper/zookeeper-3.4.5/zookeeper-3.4.5.tar.gz"
DEFAULT_ZOOKEEPER_FETCH_METHOD="cp"

########################################################################################################################
# CONFIG KEYS: The default values can be overwritten by the following keys in tools/wally.conf
########################################################################################################################

KEY_PSERVER_DESTINATION_DIRECTORY="deploy.dest.dir"
KEY_PSERVER_STAGING_DIRECTORY="deploy.staging.dir"
KEY_PSERVER_STAGING_HOST="deploy.staging.host"
KEY_ZOOKEEPER_INSTALL_DIR="deploy.zookeeper.install.dir"
KEY_ZOOKEEPER_DATA_DIR="deploy.zookeeper.data.dir"
KEY_PSERVER_CONFIG_FILE="pserver.config.file"
KEY_ZOOKEEPER_DIST_URL="zookeeper.dist.url"
KEY_ZOOKEEPER_FETCH_METHOD="zookeeper.fetch.method"

########################################################################################################################
# PATHS AND CONFIG
########################################################################################################################

# grab default env

SLAVES_FILE=${PSERVER_CONF_DIR}/slaves
ZOOKEEPERS_FILE=${PSERVER_CONF_DIR}/zookeepers
PSERVER_TOOLS_DIR=$PSERVER_ROOT_DIR_MANGLED/tools
YAML_DEPLOY_CONF=${PSERVER_CONF_DIR}/deploy.conf

PSERVER_ROOT_DIR_NAME=$(basename $(cd ${PSERVER_ROOT_DIR}; pwd))
PSERVER_DIST_ARCHIVE_FILENAME="${PSERVER_ROOT_DIR_NAME}.tar.gz"

########################################################################################################################
# ENVIRONMENT VARIABLES
########################################################################################################################

# Define PSERVER_DESTINATION_DIRECTORY if it is not already set
if [ -z "${PSERVER_DESTINATION_DIRECTORY}" ]; then
    PSERVER_DESTINATION_DIRECTORY=$(readFromConfig ${KEY_PSERVER_DESTINATION_DIRECTORY} ${DEFAULT_PSERVER_DESTINATION_DIRECTORY} ${YAML_DEPLOY_CONF})
fi

# Define PSERVER_STAGING_DIRECTORY if it is not already set
if [ -z "${PSERVER_STAGING_DIRECTORY}" ]; then
    PSERVER_STAGING_DIRECTORY=$(readFromConfig ${KEY_PSERVER_STAGING_DIRECTORY} ${DEFAULT_PSERVER_STAGING_DIRECTORY} ${YAML_DEPLOY_CONF})
fi

# Define PSERVER_STAGING_HOST if it is not already set
if [ -z "${PSERVER_STAGING_HOST}" ]; then
    PSERVER_STAGING_HOST=$(readFromConfig ${KEY_PSERVER_STAGING_HOST} ${DEFAULT_PSERVER_STAGING_HOST} ${YAML_DEPLOY_CONF})
fi

if [ -z "${PSERVER_CONFIG_FILE}" ]; then
    PSERVER_CONFIG_FILE=$(readFromConfig ${KEY_PSERVER_CONFIG_FILE} ${DEFAULT_PSERVER_CONFIG_FILE} ${YAML_DEPLOY_CONF})
fi

PSERVER_DESTINATION_LOG_DIR=$PSERVER_DESTINATION_DIRECTORY/$PSERVER_ROOT_DIR_NAME/log
PSERVER_STAGING_LOG_DIR=$PSERVER_STAGING_DIRECTORY/$PSERVER_ROOT_DIR_NAME/log

if [ -z "${ZOOKEEPER_INSTALL_DIR}" ]; then
    ZOOKEEPER_INSTALL_DIR=$(readFromConfig ${KEY_ZOOKEEPER_INSTALL_DIR} ${DEFAULT_ZOOKEEPER_INSTALL_DIR} ${YAML_DEPLOY_CONF})
fi

if [ -z "${ZOOKEEPER_DATA_DIR}" ]; then
    ZOOKEEPER_DATA_DIR=$(readFromConfig ${KEY_ZOOKEEPER_DATA_DIR} ${DEFAULT_ZOOKEEPER_DATA_DIR} ${YAML_DEPLOY_CONF})
fi

if [ -z "${ZOOKEEPER_DIST_URL}" ]; then
    ZOOKEEPER_DIST_URL=$(readFromConfig ${KEY_ZOOKEEPER_DIST_URL} ${DEFAULT_ZOOKEEPER_DIST_URL} ${YAML_DEPLOY_CONF})
fi

if [ -z "${ZOOKEEPER_FETCH_METHOD}" ]; then
    ZOOKEEPER_FETCH_METHOD=$(readFromConfig ${KEY_ZOOKEEPER_FETCH_METHOD} ${DEFAULT_ZOOKEEPER_FETCH_METHOD} ${YAML_DEPLOY_CONF})
fi

ZOOKEEPER_LOG_DIR=${ZOOKEEPER_INSTALL_DIR}/logs
ZOOKEEPER_FILE_NAME=$(basename ${ZOOKEEPER_DIST_URL})
ZOOKEEPER_DIR_NAME=$(basename ${ZOOKEEPER_FILE_NAME} .tar.gz)
ACTUAL_ZOOKEEPER_INSTALL_DIR="${ZOOKEEPER_INSTALL_DIR}/${ZOOKEEPER_DIR_NAME}"
ZOOKEEPER_CONFIG_FILE="${ACTUAL_ZOOKEEPER_INSTALL_DIR}/conf/zoo.cfg"
ZOOKEEPER_LOG_CONFIG_FILE="${ACTUAL_ZOOKEEPER_INSTALL_DIR}/conf/log4j.properties"
