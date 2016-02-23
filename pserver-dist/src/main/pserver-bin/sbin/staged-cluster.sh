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

FILE="staged-cluster"
if [ ! -z ${1+x} ] && [ -z ${CMD+x} ]; then
    CMD=$1
    shift
fi

if [ -z ${PSERVER_ROOT_DIR+x} ] || [ -z "${PSERVER_ROOT_DIR}" ]; then
    PWD=$(dirname "$0"); PWD=$(cd "${PWD}"; pwd);
    PSERVER_ROOT_DIR=$(cd "${PWD}/.."; pwd)
fi

if [ -z ${ENV+x} ] || [ -z "${ENV}" ]; then
    . "${PSERVER_ROOT_DIR}/env/env.sh"
fi

set -eu

case ${CMD} in

pserver-start)

        ssh -n $STAGING_SSH_OPTS ${PSERVER_STAGING_HOST} -- "$PSERVER_STAGING_DIRECTORY/$PSERVER_ROOT_DIR_NAME/sbin/cluster.sh pserver-start ${STAGING_PARAM_FORWARDING}"
        ;;

pserver-stop)

        ssh -n $STAGING_SSH_OPTS ${PSERVER_STAGING_HOST} -- "$PSERVER_STAGING_DIRECTORY/$PSERVER_ROOT_DIR_NAME/sbin/cluster.sh pserver-stop ${STAGING_PARAM_FORWARDING}"
        ;;

pserver-stage)

        $PSERVER_ROOT_DIR/sbin/local.sh pserver-package
        echo "[NOTICE][${PSERVER_STAGING_HOST}] Wiping target directory ..."
        ssh -n $STAGING_SSH_OPTS ${PSERVER_STAGING_HOST} -- "rm -rf ${PSERVER_STAGING_DIRECTORY}; mkdir -p ${PSERVER_STAGING_DIRECTORY}"
        echo "[NOTICE][${PSERVER_STAGING_HOST}] Transfering archive ..."
        scp -q $STAGING_SSH_OPTS ${PSERVER_ROOT_DIR}/../${PSERVER_DIST_ARCHIVE_FILENAME} ${PSERVER_STAGING_HOST}:${PSERVER_STAGING_DIRECTORY}/${PSERVER_DIST_ARCHIVE_FILENAME}
        echo "[NOTICE][${PSERVER_STAGING_HOST}] Extracting ..."
        ssh -n $STAGING_SSH_OPTS ${PSERVER_STAGING_HOST} -- "cd ${PSERVER_STAGING_DIRECTORY}; tar xzf ${PSERVER_DIST_ARCHIVE_FILENAME} -m"
        ;;

pserver-deploy)
        $PSERVER_ROOT_DIR/sbin/local.sh pserver-package
        echo "[NOTICE][${PSERVER_STAGING_HOST}] Wiping target directory ..."
        ssh -n $STAGING_SSH_OPTS ${PSERVER_STAGING_HOST} -- "rm -rf ${PSERVER_STAGING_DIRECTORY}; mkdir -p ${PSERVER_STAGING_DIRECTORY}"
        echo "[NOTICE][${PSERVER_STAGING_HOST}] Transfering archive ..."
        scp -q $STAGING_SSH_OPTS ${PSERVER_ROOT_DIR}/../${PSERVER_DIST_ARCHIVE_FILENAME} ${PSERVER_STAGING_HOST}:${PSERVER_STAGING_DIRECTORY}/${PSERVER_DIST_ARCHIVE_FILENAME}
        echo "[NOTICE][${PSERVER_STAGING_HOST}] Extracting ..."
        ssh -n $STAGING_SSH_OPTS ${PSERVER_STAGING_HOST} -- "cd ${PSERVER_STAGING_DIRECTORY}; tar xzf ${PSERVER_DIST_ARCHIVE_FILENAME} -m"
        echo "[NOTICE][${PSERVER_STAGING_HOST}] Deploying pserver from staging to nodes ..."
        ssh -n $STAGING_SSH_OPTS ${PSERVER_STAGING_HOST} -- "${PSERVER_STAGING_DIRECTORY}/${PSERVER_ROOT_DIR_NAME}/sbin/cluster.sh pserver-deploy -called-from-staging ${STAGING_PARAM_FORWARDING}"
        ;;

fetch-logs)

        function fetchLogs() {
            # create tmp dir for logs
            PSERVER_LOG_DIR_TMP="${PSERVER_LOG_DIR}/tmp"
            if [ -e "${PSERVER_LOG_DIR_TMP}" ]; then
                rm -rf "${PSERVER_LOG_DIR_TMP}" 2>/dev/null || true
            fi
            mkdir -p "${PSERVER_LOG_DIR_TMP}" 2>/dev/null || true
            # gather logs from at to staging server
            ssh -n $STAGING_SSH_OPTS ${PSERVER_STAGING_HOST} -- "${PSERVER_STAGING_DIRECTORY}/${PSERVER_ROOT_DIR_NAME}/sbin/cluster.sh fetch-logs ${STAGING_PARAM_FORWARDING}"
            # copy log files from staging to local
            scp -q $STAGING_SSH_OPTS ${PSERVER_STAGING_HOST}:"${PSERVER_DESTINATION_LOG_DIR}/*" ${PSERVER_LOG_DIR_TMP}/. 2> /dev/null
            # now append diffs from tmp files to existing ones
            process_logs
            rm -rf "${PSERVER_LOG_DIR_TMP}" 2>/dev/null || true
            # done
        }

        if [ -z ${PARAM_FETCH_LOG_INTERVAL+x} ]; then
            fetchLogs
        else
            while true; do
                fetchLogs
                sleep "${PARAM_FETCH_LOG_INTERVAL}"
            done
        fi
        ;;

zookeeper-setup)
		
		ssh -n $STAGING_SSH_OPTS ${PSERVER_STAGING_HOST} -- "${PSERVER_STAGING_DIRECTORY}/${PSERVER_ROOT_DIR_NAME}/sbin/cluster.sh zookeeper-setup ${STAGING_PARAM_FORWARDING}"
		;;

zookeeper-start)
        
        ssh -n $STAGING_SSH_OPTS ${PSERVER_STAGING_HOST} -- "${PSERVER_STAGING_DIRECTORY}/${PSERVER_ROOT_DIR_NAME}/sbin/cluster.sh zookeeper-start ${STAGING_PARAM_FORWARDING}"
        ;;

zookeeper-stop)
        
        ssh -n $STAGING_SSH_OPTS ${PSERVER_STAGING_HOST} -- "${PSERVER_STAGING_DIRECTORY}/${PSERVER_ROOT_DIR_NAME}/sbin/cluster.sh zookeeper-stop ${STAGING_PARAM_FORWARDING}"
        ;;
    
*)
        echo "Usage: staged-cluster.sh [pserver-start|pserver-stop|pserver-deploy|fetch-logs|zookeeper-setup|zookeeper-start|zookeeper-stop] [options ...]"
        ;;

esac

if [ "$(type -t profile_finish_hook 2>/dev/null)" == "function" ]; then
	profile_finish_hook
fi
