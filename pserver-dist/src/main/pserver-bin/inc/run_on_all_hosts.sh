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

if [ -z ${ENV+x} ] || [ -z "${ENV}" ]; then
    . "${PSERVER_ROOT_DIR}/env/env.sh"
fi

# ${HOSTLIST} may be set to the zookeepers file to ??? over zookeeper hosts
if [ -z ${HOSTLIST+x} ] || [ -z "${HOSTLIST}" ]; then
    HOSTLIST="${SLAVES_FILE}"
fi

if [ "$(mtxType -t run_on_host 2>/dev/null)" != "function" ]; then
	echo "[ERROR][${HOSTNAME}] You have to define a function 'run_on_host <HOST>' before including this snippet"
    exit 1
fi

if [ ! -f $HOSTLIST ]; then
    echo "[ERROR][${HOSTNAME}] $HOSTLIST is not a valid slave list"
    exit 1
fi

GOON=true
while $GOON
do
    read HOST || GOON=false
    if [ -n "${HOST}" ]; then
        run_on_host "${HOST}" || true
    fi
done < $HOSTLIST