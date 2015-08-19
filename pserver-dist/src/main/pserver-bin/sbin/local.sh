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

FILE="local"
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

        PSERVER_NODE_CLASSPATH=`manglePathList $(constructNodeClassPath)`

		log=$PSERVER_LOG_DIR/pserver-$PSERVER_IDENT_STRING-node-$HOSTNAME.log
		out=$PSERVER_LOG_DIR/pserver-$PSERVER_IDENT_STRING-node-$HOSTNAME.out
		pid=$PSERVER_PID_DIR/pserver-$PSERVER_IDENT_STRING-node.pid
		log_setting="-Dlog.file="$log" -Dlog4j.configuration=file:"$PSERVER_CONF_DIR"/log4j.properties"

		JVM_ARGS="$JVM_ARGS -XX:+UseParNewGC -XX:NewRatio=8 -XX:PretenureSizeThreshold=64m -Xms"$PSERVER_NODE_HEAP"m -Xmx"$PSERVER_NODE_HEAP"m"

		# TODO: this is only because profiles are set in code. When profiles are set by params, this can be removed
		PSERVER_PROFILE="default"
		if [ ! -z ${PARAM_START_REMOTE+x} ]; then
			PSERVER_PROFILE=$ENV_PROFILE
		fi

		mkdir -p "$PSERVER_PID_DIR"
        if [ -f $pid ]; then
            if kill -0 `cat $pid` > /dev/null 2>&1; then
                echo "[NOTICE][${HOSTNAME}] pserver node running as process `cat $pid`.  Stop it first."
                exit 1
            fi
        fi

        # Rotate log files
        rotateLogFile $log
        rotateLogFile $out

        echo "[NOTICE][${HOSTNAME}] Starting pserver node ... "
        $JAVA_RUN $JVM_ARGS $PSERVER_OPTS $log_setting -classpath $PSERVER_NODE_CLASSPATH de.tuberlin.pserver.node.PServerMain --config-dir=$PSERVER_CONF_DIR --profile=$PSERVER_PROFILE > "$out" 2>&1 < /dev/null &
        echo $! > $pid
        ;;

pserver-stop)
        
        pid=$PSERVER_PID_DIR/pserver-$PSERVER_IDENT_STRING-node.pid
        if [ -f $pid ]; then
            if kill -0 `cat $pid` > /dev/null 2>&1; then
                echo "[NOTICE][${HOSTNAME}] Stopping pserver node ..."
                kill `cat $pid`
            else
                >&2 echo "[NOTICE][${HOSTNAME}] pserver is not running. "
            fi
        else
            >&2 echo "[NOTICE][${HOSTNAME}] pid file of pserver not found. Nothing to do. "
        fi
        ;;
        
pserver-package)

		echo "[NOTICE][$(hostname)] Creating archive: ${PSERVER_DIST_ARCHIVE_FILENAME} ... "
		pushd ${PSERVER_ROOT_DIR}/.. > /dev/null
		if [ -f ${PSERVER_DIST_ARCHIVE_FILENAME} ]; then
			rm ${PSERVER_DIST_ARCHIVE_FILENAME}
		fi
		tar czf "${PSERVER_DIST_ARCHIVE_FILENAME}" "${PSERVER_ROOT_DIR_NAME}"
		popd > /dev/null
		;;

zookeeper-setup)

		function create_zookeeper_host_list {
			NODE_COUNT=1
			RESULT=""
			PORT=2181
			if [ $# -ge 1 ] && [ ! -z "$1" ]; then
				PORT=$1
			fi
			LINE_SEPARATOR="\n"
			if [ $# -ge 2 ] && [ ! -z "$2" ]; then
				LINE_SEPARATOR=$2
			fi
			for HOST in $(cat ${ZOOKEEPERS_FILE}); do
				if [ ! -z "${HOST}" ]; then
					if [ $NODE_COUNT -eq 1 ]; then
						RESULT="server.${NODE_COUNT}=${HOST}:${PORT}"
					else
						RESULT="${RESULT}${LINE_SEPARATOR}server.${NODE_COUNT}=${HOST}:${PORT}"
					fi
					# $PARAM_COUNT_OF_ZOOKEEPER_NODES is profile specific
					# if [ $NODE_COUNT -ge $PARAM_COUNT_OF_ZOOKEEPER_NODES ]; then
					# 	break
					# fi
					NODE_COUNT=$((NODE_COUNT+1))
				fi
			done
			echo -e $RESULT
		}

		if [ -z ${PARAM_ZOOKEEPER_MYID+x} ] || [ -z "${PARAM_ZOOKEEPER_MYID}" ]; then
			echo "[ERROR] You must specify a unique zookeeper id with parameter -zid"
		fi

		WIPE=1
		if [ $WIPE -eq 1  ]; then
			echo "[NOTICE][$(hostname)] Wiping zookeeper installation ..."
			# stop possibly running zookeeper node first
			if [ -f "${ZOOKEEPER_DATA_DIR}/zookeeper_server.pid" ]; then
				echo "[NOTICE][$(hostname)] Zookeeper pid exists. Stopping ..."
				${ACTUAL_ZOOKEEPER_INSTALL_DIR}/bin/zkServer.sh stop 2>&1 | xargs -L 1 -I '{}' echo [NOTICE][$(hostname)] {}
			fi
			rm -rf ${ZOOKEEPER_INSTALL_DIR}
			rm -rf ${ZOOKEEPER_DATA_DIR}
		fi

		if [ ! -d "${ZOOKEEPER_INSTALL_DIR}" ]; then
			mkdir -p ${ZOOKEEPER_INSTALL_DIR}
			echo "[NOTICE][$(hostname)] Zookeeper installation not found. Installing ..."
			case "${ZOOKEEPER_FETCH_METHOD}" in
				wget)
					wget -q -O "${ZOOKEEPER_INSTALL_DIR}/${ZOOKEEPER_FILE_NAME}" "${ZOOKEEPER_DIST_URL}"
					;;
				cp)
					cp "${ZOOKEEPER_DIST_URL}" "${ZOOKEEPER_INSTALL_DIR}/${ZOOKEEPER_FILE_NAME}"
					;;
				*)
					echo "[ERROR][$(hostname)] Unkown zookeeper fetch method: ${ZOOKEEPER_FETCH_METHOD}. Check config."
					exit 1
					;;
			esac
			tar xzf ${ZOOKEEPER_INSTALL_DIR}/${ZOOKEEPER_FILE_NAME} -C ${ZOOKEEPER_INSTALL_DIR}
			rm ${ZOOKEEPER_INSTALL_DIR}/${ZOOKEEPER_FILE_NAME}
			mv "${ACTUAL_ZOOKEEPER_INSTALL_DIR}/conf/zoo_sample.cfg" ${ZOOKEEPER_CONFIG_FILE}
		fi

		echo "[NOTICE][$(hostname)] Configuring ..."

		if [ ! -d "${ZOOKEEPER_DATA_DIR}" ]; then
			mkdir -p ${ZOOKEEPER_DATA_DIR}
		fi

		# remove all server entries from zookeeper config file
		sed '/^server/d' "${ZOOKEEPER_CONFIG_FILE}" > "${ZOOKEEPER_CONFIG_FILE}.tmp" && mv "${ZOOKEEPER_CONFIG_FILE}.tmp" "${ZOOKEEPER_CONFIG_FILE}"
		# add zookeeper nodes to config
		create_zookeeper_host_list "2888:3888" >> "${ZOOKEEPER_CONFIG_FILE}"
		# remove dataDir from zookeeper config
		sed '/^dataDir/d' "${ZOOKEEPER_CONFIG_FILE}" > "${ZOOKEEPER_CONFIG_FILE}.tmp" && mv "${ZOOKEEPER_CONFIG_FILE}.tmp" "${ZOOKEEPER_CONFIG_FILE}"
		# add dataDir to zookeeper config
		echo "dataDir=${ZOOKEEPER_DATA_DIR}" >> "${ZOOKEEPER_CONFIG_FILE}"
		# set myid file
		echo "${PARAM_ZOOKEEPER_MYID}" > ${ZOOKEEPER_DATA_DIR}/myid

		# set log settings
		if [ ! -d ${ZOOKEEPER_LOG_DIR} ]; then
			mkdir ${ZOOKEEPER_LOG_DIR}
		fi
		# zookeeper doesn't seems to use these, but I'll leave them in case it does somewhen
		log=${ZOOKEEPER_LOG_DIR}/zookeeper-$PSERVER_IDENT_STRING-node-$HOSTNAME.log
		trace=${ZOOKEEPER_LOG_DIR}/zookeeper-$PSERVER_IDENT_STRING-node-$HOSTNAME.trace
		sed '/^zookeeper.log.dir/d;/^zookeeper.log.file/d;/^zookeeper.tracelog.dir/d;/^zookeeper.tracelog.file/d' "${ZOOKEEPER_LOG_CONFIG_FILE}" > "${ZOOKEEPER_LOG_CONFIG_FILE}.tmp" && mv "${ZOOKEEPER_LOG_CONFIG_FILE}.tmp" "${ZOOKEEPER_LOG_CONFIG_FILE}"
		echo "zookeeper.log.dir=${ZOOKEEPER_LOG_DIR}" >> ${ZOOKEEPER_LOG_CONFIG_FILE}
		echo "zookeeper.tracelog.dir=${ZOOKEEPER_LOG_DIR}" >> ${ZOOKEEPER_LOG_CONFIG_FILE}
		echo "zookeeper.log.file=${log}" >> ${ZOOKEEPER_LOG_CONFIG_FILE}
		echo "zookeeper.tracelog.file=${trace}" >> ${ZOOKEEPER_LOG_CONFIG_FILE}
		;;

pserver-add-zookeeper-config)
		
		function create_zookeeper_host_struct {
			RESULT="zookeeper{servers=["
			PORT=2181
			for HOST in $(cat ${ZOOKEEPERS_FILE}); do
				if [ ! -z "${HOST}" ]; then
					RESULT="${RESULT}{host=\"${HOST}\",port=${PORT}}"
					# currently, the pserver only supports a single zookeeper host
					break
				fi
			done
			RESULT="${RESULT}]}"
			echo $RESULT
		}

		# append zookeeper pserver config. This is a bit hacky: typesafe replaces existing entries if they
		# appear later in the file. So by appending the list, we effectively "overwrite" a possibly existing host list
		echo "" >> "${PSERVER_CONF_DIR}/${PSERVER_CONFIG_FILE}" # new line to be safe
		create_zookeeper_host_struct >> "${PSERVER_CONF_DIR}/${PSERVER_CONFIG_FILE}"
		;;

zookeeper-start)
		pushd "${ZOOKEEPER_LOG_DIR}" > /dev/null
		rotateLogFile "${ZOOKEEPER_LOG_DIR}/zookeeper.out"
		${ACTUAL_ZOOKEEPER_INSTALL_DIR}/bin/zkServer.sh start 2>&1 | xargs -L 1 -I '{}' echo [NOTICE][$(hostname)] {}
        popd > /dev/null
        ;;

zookeeper-stop)
        pushd "${ZOOKEEPER_LOG_DIR}" > /dev/null
		${ACTUAL_ZOOKEEPER_INSTALL_DIR}/bin/zkServer.sh stop 2>&1 | xargs -L 1 -I '{}' echo [NOTICE][$(hostname)] {}
        popd > /dev/null
        ;;
    
*)
        echo "Usage: local.sh [pserver-start|pserver-stop|pserver-package|zookeeper-setup|pserver-add-zookeeper-config|zookeeper-start|zookeeper-stop] [options ...]"
        ;;

esac

if [ "$(type -t profile_finish_hook 2>/dev/null)" == "function" ]; then
	profile_finish_hook
fi
