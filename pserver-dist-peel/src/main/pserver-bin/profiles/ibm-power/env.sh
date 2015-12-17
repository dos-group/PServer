#!/bin/bash
########################################################################################################################
# 
#  Copyright (C) 2015 by the pserver project
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

function profile_parse_arg() {
	if [ $# -eq 0 ]; then
		echo "[ERROR][${HOSTNAME}] No argument was passed to parse by profile"
		exit 1
	fi
	case "${1}" in
		-f|--first-node)
			if [ -z ${2+x} ]; then
				echo "[ERROR][${HOSTNAME}] Value expected for parameter '-f', but nothing given"
				exit 1
			fi
			PARAM_FIRST_NODE="${2}"
			STAGING_PARAM_FORWARDING="${STAGING_PARAM_FORWARDING} -f ${PARAM_FIRST_NODE}"
			;;
		-c|--count-of-nodes)
			if [ -z ${2+x} ]; then
				echo "[ERROR][${HOSTNAME}] Value expected for parameter '-c', but nothing given"
				exit 1
			fi
			PARAM_COUNT_OF_NODES="${2}"
			STAGING_PARAM_FORWARDING="${STAGING_PARAM_FORWARDING} -c ${PARAM_COUNT_OF_NODES}"
			;;
		-z|--count-of-zookeeper-nodes)
			if [ -z ${2+x} ]; then
				echo "[ERROR][${HOSTNAME}] Value expected for parameter '-z', but nothing given"
				exit 1
			fi
			PARAM_COUNT_OF_ZOOKEEPER_NODES="${2}"
			STAGING_PARAM_FORWARDING="${STAGING_PARAM_FORWARDING} -z ${PARAM_COUNT_OF_ZOOKEEPER_NODES}"
			;;
		*)
			return 1
			;;
	esac
	return 0
}

function check_args_for_slaves_gen() {
	# check if args are set
	if [ -z ${PARAM_FIRST_NODE+x} ] && [ -z ${PARAM_COUNT_OF_NODES+x} ]; then
		echo "[ERROR][${HOSTNAME}] No node range provided. Please specify the first node id with parameter '-f' and the count of nodes with parameter '-c'"
		exit 1
	elif [ -z ${PARAM_FIRST_NODE+x} ] && [ ! -z ${PARAM_COUNT_OF_NODES+x} ]; then
		echo "[ERROR][${HOSTNAME}] Incomplete node range provided. Please provide first node id with parameter -f"
		exit 1
	elif [ ! -z ${PARAM_FIRST_NODE+x} ] && [ -z ${PARAM_COUNT_OF_NODES+x} ]; then
		echo "[ERROR][${HOSTNAME}] Incomplete node range provided. Please provide count of nodes with parameter -c"
		exit 1
	fi

	# Check if args are numbers in allowed ranges
	pattern='^([2-9])$'
	if ! echo ${PARAM_FIRST_NODE} | egrep -q $pattern; then
		echo "[ERROR][${HOSTNAME}] first node (parameter -f) must be a number in the range [2, 9], but '${PARAM_FIRST_NODE}' was given."
		exit 1
	fi
	node_range=$((PARAM_FIRST_NODE+PARAM_COUNT_OF_NODES-1))
	if [ ${node_range} -gt 9 ]; then
		echo "[ERROR][${HOSTNAME}] The last node on the cluster that you can allocate has id 9. The range you provided exeeds that limit."
		exit 1
	fi
}

function gen_slaves_hostlist() {
	check_args_for_slaves_gen
	if [ -f "${SLAVES_FILE}" ]; then
		rm "${SLAVES_FILE}"
	fi
	touch "${SLAVES_FILE}"
	for i in $(seq ${PARAM_FIRST_NODE} $((${PARAM_FIRST_NODE} + ${PARAM_COUNT_OF_NODES} - 1))); do
		echo "ibm-power-$(printf "%d" $i).dima.tu-berlin.de" >> ${SLAVES_FILE}
	done
}

function check_args_for_zookeepers_gen() {
	check_args_for_slaves_gen
	# check if args are set
	if [ -z ${PARAM_COUNT_OF_ZOOKEEPER_NODES+x} ] || [ -z "${PARAM_COUNT_OF_ZOOKEEPER_NODES}" ]; then
		echo "[ERROR][${HOSTNAME}] No zookeeper node count. Please specify count of nodes with parameter '-z' you want to use as zookeepers"
		exit 1
	fi

	if [ ${PARAM_COUNT_OF_ZOOKEEPER_NODES} -gt ${PARAM_COUNT_OF_NODES} ]; then
		echo "[ERROR][${HOSTNAME}] count of zookeeper nodes (${PARAM_COUNT_OF_ZOOKEEPER_NODES}) must be smaller or equal than the range of nodes (${PARAM_COUNT_OF_NODES})"
		exit 1
	fi
}

function gen_zookeeper_hostlist() {
	check_args_for_zookeepers_gen
	if [ -f "${ZOOKEEPERS_FILE}" ]; then
		rm "${ZOOKEEPERS_FILE}"
	fi
	touch "${ZOOKEEPERS_FILE}"
	for i in $(seq ${PARAM_FIRST_NODE} $((${PARAM_FIRST_NODE} + ${PARAM_COUNT_OF_ZOOKEEPER_NODES} - 1))); do
		echo "ibm-power-$(printf "%d" $i).dima.tu-berlin.de" >> ${ZOOKEEPERS_FILE}
	done
}

function profile_env() {
	# depending on the called file/cmd, generate slaves/zookeeper hostlist
	case "${FILE}" in
		cluster)
			gen_slaves_hostlist
			case "${CMD}" in
			zookeeper*)
				gen_zookeeper_hostlist
				;;
			esac
		;;
		local)
			case "${CMD}" in
				zookeeper-setup|pserver-add-zookeeper-config)
					gen_zookeeper_hostlist
				;;
			esac
		;;
	esac
}
