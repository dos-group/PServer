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

if [ -z ${PSERVER_ROOT_DIR+x} ] || [ -z "${PSERVER_ROOT_DIR}" ]; then
    PWD=$(dirname "$0"); PWD=$(cd "${PWD}"; pwd);
    PSERVER_ROOT_DIR=$(cd "${PWD}/.."; pwd)
fi

# build
mvn clean install
# deploy pserver
${PSERVER_ROOT_DIR}/sbin/cluster.sh "pserver-deploy" "$@"
# setup zookeeper
${PSERVER_ROOT_DIR}/sbin/cluster.sh "zookeeper-setup" "$@"
# start zookeeper
${PSERVER_ROOT_DIR}/sbin/cluster.sh "zookeeper-start" "$@"
# stop pserver
${PSERVER_ROOT_DIR}/sbin/cluster.sh "pserver-start" "$@"