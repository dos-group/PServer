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

FILE="cluster"
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

        function run_on_host() {
            HOST=$1
            ssh -n $PSERVER_SSH_OPTS $HOST -- "nohup /bin/bash $PSERVER_DESTINATION_DIRECTORY/$PSERVER_ROOT_DIR_NAME/sbin/local.sh pserver-start -start-remote &"
        }
        . "${PSERVER_ROOT_DIR}/inc/run_on_all_hosts.sh"
        ;;

pserver-stop)

        function run_on_host() {
            HOST=$1
            ssh -n $PSERVER_SSH_OPTS $HOST -- "/bin/bash $PSERVER_DESTINATION_DIRECTORY/$PSERVER_ROOT_DIR_NAME/sbin/local.sh pserver-stop"
        }
        . "${PSERVER_ROOT_DIR}/inc/run_on_all_hosts.sh"
        ;;

pserver-deploy)

        if [ -z ${PARAM_CALLED_FROM_STAGING+x} ]; then
            ARCHIVE_FILE_ROOT_DIR="${PSERVER_ROOT_DIR}/.."
            $PSERVER_ROOT_DIR/sbin/local.sh pserver-package
        else
            ARCHIVE_FILE_ROOT_DIR="${PSERVER_DESTINATION_DIRECTORY}"
        fi
        
        function run_on_host() {
            HOST=$1
            # stop possibly running pserver node (just to be safe)
            ssh -n $PSERVER_SSH_OPTS $HOST -- "/bin/bash $PSERVER_DESTINATION_DIRECTORY/$PSERVER_ROOT_DIR_NAME/sbin/local.sh pserver-stop 2>/dev/null" 2>/dev/null || true
            echo "[NOTICE][$HOST] Wiping target directory ..."
            ssh -n $PSERVER_SSH_OPTS ${HOST} -- "rm -rf ${PSERVER_DESTINATION_DIRECTORY}; mkdir -p ${PSERVER_DESTINATION_DIRECTORY}"
            echo "[NOTICE][$HOST] Transfering archive file ..."
            scp -q $PSERVER_SSH_OPTS "${ARCHIVE_FILE_ROOT_DIR}/${PSERVER_DIST_ARCHIVE_FILENAME}" ${HOST}:${PSERVER_DESTINATION_DIRECTORY}/${PSERVER_DIST_ARCHIVE_FILENAME}
            echo "[NOTICE][$HOST] Extracting ..."
            ssh -n $PSERVER_SSH_OPTS ${HOST} -- "cd ${PSERVER_DESTINATION_DIRECTORY}; tar xzf ${PSERVER_DIST_ARCHIVE_FILENAME} -m"
        }
        . "${PSERVER_ROOT_DIR}/inc/run_on_all_hosts.sh"
        ;;

fetch-logs)

        function run_on_host() {
            echo "[NOTICE] Fetching logs ..."
            HOST=$1
            HOSTNAME=$(ssh -n ${PSERVER_SSH_OPTS} ${HOST} -- "hostname")
            # the zookeeper logfile is just called "zookeeper.out". We want to replace that filename with ${ZOOKEEPER_LOG_FILENAME}
            ZOOKEEPER_LOG_FILENAME="zookeeper-${PSERVER_IDENT_STRING}-node-${HOSTNAME}.out"
            # print the log files we are about to fetch and replace a possibly existing zookeeper logfile with the intended file name right away
            ssh -n ${PSERVER_SSH_OPTS} ${HOST} -- "find \"${PSERVER_DESTINATION_LOG_DIR}\" -type f -print0 2> /dev/null; find \"${ZOOKEEPER_LOG_DIR}\" -type f -print0 2> /dev/null" | xargs -0 -I '{}' sh -c 'echo [NOTICE]['"${HOST}"'] $(basename {})' | sed 's/zookeeper.out\(.*\)$/'"${ZOOKEEPER_LOG_FILENAME}"\\1'/g'
            # create tmp dir for logs
            PSERVER_LOG_DIR_TMP="${PSERVER_LOG_DIR}/tmp"
            if [ -e "${PSERVER_LOG_DIR_TMP}" ]; then
                rm -rf "${PSERVER_LOG_DIR_TMP}" 2>/dev/null || true
            fi
            mkdir -p "${PSERVER_LOG_DIR_TMP}" 2>/dev/null || true
            # copy log files to tmp
            scp -q $PSERVER_SSH_OPTS ${HOST}:"${PSERVER_DESTINATION_LOG_DIR}/* ${ZOOKEEPER_LOG_DIR}/*" ${PSERVER_LOG_DIR_TMP}/. 2> /dev/null || true
            # if the current host contained zookeeper logs
            for f in "${PSERVER_LOG_DIR_TMP}/"zookeeper.out*; do
                if [ -f "${f}" ]; then
                    mv "${f}" "${PSERVER_LOG_DIR_TMP}/$(basename ${f} | sed 's/zookeeper.out\(.*\)$/'"${ZOOKEEPER_LOG_FILENAME}"\\1'/g')"
                fi
            done
            # now append diffs from tmp files to existing ones
            process_logs
            rm -rf "${PSERVER_LOG_DIR_TMP}" 2>/dev/null || true
            # done
        }

        function fetchLogs() {
            . "${PSERVER_ROOT_DIR}/inc/run_on_all_hosts.sh"
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
        
        ZOOKEEPER_MYID=1
        function run_on_host() {
            HOST=$1
            ssh -n $PSERVER_SSH_OPTS ${HOST} -- "${PSERVER_DESTINATION_DIRECTORY}/${PSERVER_ROOT_DIR_NAME}/sbin/local.sh zookeeper-setup -zid ${ZOOKEEPER_MYID} ${STAGING_PARAM_FORWARDING}"
            ZOOKEEPER_MYID=$((ZOOKEEPER_MYID+1))
        }
        HOSTLIST="${ZOOKEEPERS_FILE}"
        . "${PSERVER_ROOT_DIR}/inc/run_on_all_hosts.sh"
        function run_on_host() {
            HOST=$1
            ssh -n $PSERVER_SSH_OPTS ${HOST} -- "${PSERVER_DESTINATION_DIRECTORY}/${PSERVER_ROOT_DIR_NAME}/sbin/local.sh pserver-add-zookeeper-config ${STAGING_PARAM_FORWARDING}"
        }
        HOSTLIST="${SLAVES_FILE}"
        . "${PSERVER_ROOT_DIR}/inc/run_on_all_hosts.sh"
        ;;

zookeeper-start)

        NUM_ZOOKEEPERS=$(cat "${ZOOKEEPERS_FILE}" | sed '/^\s*$/d' | wc -l)
        CURRENT_NODE=1
        function run_on_host() {
            HOST=$1
            if [ $CURRENT_NODE -lt $NUM_ZOOKEEPERS ]; then
                ssh -n $PSERVER_SSH_OPTS ${HOST} -- "${PSERVER_DESTINATION_DIRECTORY}/${PSERVER_ROOT_DIR_NAME}/sbin/local.sh zookeeper-start --zookeeper-do-not-set-numnodes"
            else
                ssh -n $PSERVER_SSH_OPTS ${HOST} -- "${PSERVER_DESTINATION_DIRECTORY}/${PSERVER_ROOT_DIR_NAME}/sbin/local.sh zookeeper-start"
            fi
            CURRENT_NODE=$((CURRENT_NODE+1))
        }
        HOSTLIST="${ZOOKEEPERS_FILE}"
        . "${PSERVER_ROOT_DIR}/inc/run_on_all_hosts.sh"
        ;;

zookeeper-stop)

        function run_on_host() {
            HOST=$1
            ssh -n $PSERVER_SSH_OPTS ${HOST} -- "${PSERVER_DESTINATION_DIRECTORY}/${PSERVER_ROOT_DIR_NAME}/sbin/local.sh zookeeper-stop"
        }
        HOSTLIST="${ZOOKEEPERS_FILE}"
        . "${PSERVER_ROOT_DIR}/inc/run_on_all_hosts.sh"
        ;;

reset)

        function run_on_host() {
            HOST=$1
            echo "[${HOST}] clearing logs"
            ssh -n ${PSERVER_SSH_OPTS} ${HOST} -- "rm \"${PSERVER_DESTINATION_LOG_DIR}/\"* 2&>1 > /dev/null; rm ${ZOOKEEPER_LOG_DIR}/* 2&>1 > /dev/null"
            echo "[${HOST}] clearing zookeeper data"
            ssh -n ${PSERVER_SSH_OPTS} ${HOST} -- "rm -rf \"${ZOOKEEPER_DATA_DIR}/version-2\" 2&>1 > /dev/null"
        }
        HOSTLIST="${SLAVES_FILE}"
        . "${PSERVER_ROOT_DIR}/inc/run_on_all_hosts.sh"
        ;;

*)
        echo "Usage: cluster.sh [pserver-start|pserver-stop|pserver-deploy|fetch-logs|zookeeper-setup|zookeeper-start|zookeeper-stop|clear-logs] [options ...]"
        ;;

esac

if [ "$(type -t profile_finish_hook 2>/dev/null)" == "function" ]; then
	profile_finish_hook
fi
