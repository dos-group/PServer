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

# marker that env has been included
ENV=1
# set profile
ENV_PROFILE="cloud-7"

# set root dir if not done yet
if [ -z ${PSERVER_ROOT_DIR+x} ] || [ -z "${PSERVER_ROOT_DIR}" ]; then
    PWD=$(dirname "$0"); PWD=$(cd "${PWD}"; pwd);
    PSERVER_ROOT_DIR=$(cd "${PWD}/.."; pwd)
fi

. "${PSERVER_ROOT_DIR}/env/runtime-env.sh"
. "${PSERVER_ROOT_DIR}/env/deploy-env.sh"
# this var can be set by a profile to carry profile-specific parameter from staging- on to cluster-cmds
STAGING_PARAM_FORWARDING=""
# i.e.: cluster.sh deploy -f 1 -c 5
# in the wally profile, this means the script shall create a slaves file containing 5 hosts starting with id 1 before executing the command
# when this gets staged through 'staged-cluster.sh deploy -f 1 -c 5' the profile-specific parameters 'f' and 'c' need to be forwarded:
# STAGING_PARAM_FORWARDING="-f 1 -c 5"
. "${PSERVER_ROOT_DIR}/profiles/${ENV_PROFILE}/env.sh"

while [ $# -gt 0 ]; do
	case "${1}" in
		-zid|--zookeeper-myid)
			if [ -z ${2+x} ]; then
				echo "[ERROR][${HOSTNAME}] Value expected for parameter '-zid', but nothing given"
				exit 1
			fi
			PARAM_ZOOKEEPER_MYID="${2}"
			shift 2
			;;
		-called-from-staging)
			PARAM_CALLED_FROM_STAGING=1
			shift 1
			;;
		-start-remote)
			PARAM_START_REMOTE=1
			shift 1
			;;
		-fetch-logs-interval)
			if [ -z ${2+x} ]; then
				echo "[ERROR][${HOSTNAME}] Value expected for parameter '-fetch-logs-interval', but nothing given"
				exit 1
			fi
			PARAM_FETCH_LOG_INTERVAL="${2}"
			shift 2
			;;
		*)
			# if the current profile defined the function 'profile_parse_arg' call it to parse the current arg
			if [ "$(type -t profile_parse_arg 2>/dev/null)" == "function" ]; then
				if [ -z ${2+x} ]; then
					if ! profile_parse_arg "${1}"; then
						echo "[ERROR][${HOSTNAME}] unrecognized argument '${1}'"
						exit 1
					fi
					shift
					
				else
					if ! profile_parse_arg "${1}" "${2}"; then
						echo "[ERROR][${HOSTNAME}] unrecognized argument '${1}'"
						exit 1
					fi
					shift 2
				fi
			fi
			;;
	esac
done

if [ "$(type -t profile_env 2>/dev/null)" == "function" ]; then
	profile_env
fi