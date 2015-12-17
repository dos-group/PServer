#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

constructClassPath() {
    for jarfile in $PSERVER_LIB_DIR/*.jar ; do
        if [ -z ${PSERVER_NODE_CLASSPATH+x} ] || [ -z "${PSERVER_NODE_CLASSPATH}" ]; then
            PSERVER_NODE_CLASSPATH=$jarfile;
        else
            PSERVER_NODE_CLASSPATH=$PSERVER_NODE_CLASSPATH:$jarfile
        fi
    done

    echo $PSERVER_NODE_CLASSPATH
}

# Auxilliary functionTypeName for log file rotation
rotateLogFile() {
    log=$1;
    num=$MAX_LOG_FILE_NUMBER
    if [ -f "$log" -a "$num" -gt 0 ]; then
        while [ $num -gt 1 ]; do
            prev=`expr $num - 1`
            [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
            num=$prev
        done
        mv "$log" "$log.$num";
    fi
}

# Looks up a config value by key from a simple YAML-style key-value map.
# $1: key to look up
# $2: default value to return if key does not exist
# $3: config file to read from
readFromConfig() {
    local key=$1
    local defaultValue=$2
    local configFile=$3

    # first extract the value with the given key (1st sed), then trim the result (2nd sed)
    # if a key exists multiple times, take the "last" one (tail)
    local value=`sed -n "s/^[ ]*${key}[ ]*: \([^#]*\).*$/\1/p" "${configFile}" | sed "s/^ *//;s/ *$//" | tail -n 1`

    [ -z "$value" ] && echo "$defaultValue" || echo "$value"
}

########################################################################################################################
# DEFAULT CONFIG VALUES: These values will be used when nothing has been specified in conf/flink-conf.yaml
# -or- the respective environment variables are not set.
########################################################################################################################


# WARNING !!! , these values are only used if there is nothing else is specified in
# conf/flink-conf.yaml

DEFAULT_ENV_PID_DIR="/tmp"                          # Directory to store *.pid files to
DEFAULT_ENV_JAVA_OPTS=""                            # Optional JVM args
DEFAULT_ENV_SSH_OPTS=""                             # Optional SSH parameters running in cluster mode
DEFAULT_ENV_ARCHIVE_DIR=""
DEFAULT_ENV_PROFILE="default"
DEFAULT_NODE_HEAP="1024"

########################################################################################################################
# CONFIG KEYS: The default values can be overwritten by the following keys in conf/flink-conf.yaml
########################################################################################################################

KEY_ENV_PID_DIR="env.pid.dir"
KEY_ENV_ARCHIVE_DIR="env.archive.dir"
KEY_ENV_JAVA_HOME="env.java.home"
KEY_ENV_JAVA_OPTS="env.java.opts"
KEY_ENV_SSH_OPTS="env.ssh.opts"
KEY_ENV_PROFILE="env.profile"
KEY_ENV_NODE_HEAP_MB="env.node.heap.mb"

########################################################################################################################
# PATHS AND CONFIG
########################################################################################################################

# Resolve links
this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# Convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

# Define the main directory of the flink installation
PSERVER_ROOT_DIR=`dirname "$this"`/..
PSERVER_LIB_DIR=$PSERVER_ROOT_DIR/lib
PSERVER_CONF_DIR=$PSERVER_ROOT_DIR/conf
PSERVER_BIN_DIR=$PSERVER_ROOT_DIR/bin
PSERVER_LOG_DIR=$PSERVER_ROOT_DIR/log
YAML_CONF=${PSERVER_CONF_DIR}/pserver-conf.yaml

# Directories and archive names for deployment
ARCHIVE_NAME=`cd $PSERVER_ROOT_DIR; pwd`
ARCHIVE_DIR=`dirname $ARCHIVE_NAME`
ARCHIVE=`basename $ARCHIVE_NAME`
ARCHIVE_PATH=${ARCHIVE_DIR}/${ARCHIVE}.tar.gz

########################################################################################################################
# ENVIRONMENT VARIABLES
########################################################################################################################

# read JAVA_HOME from config with no default value
MY_JAVA_HOME=$(readFromConfig ${KEY_ENV_JAVA_HOME} "" "${YAML_CONF}")
# check if config specified JAVA_HOME
if [ -z "${MY_JAVA_HOME}" ]; then
    # config did not specify JAVA_HOME. Use system JAVA_HOME
    MY_JAVA_HOME=${JAVA_HOME}
fi
# check if we have a valid JAVA_HOME and if java is not available
if [ -z "${MY_JAVA_HOME}" ] && ! type java > /dev/null 2> /dev/null; then
    echo "Please specify JAVA_HOME. Either in Flink config ./conf/flink-conf.yaml or as system-wide JAVA_HOME."
    exit 1
else
    JAVA_HOME=${MY_JAVA_HOME}
fi

if [[ -d $JAVA_HOME ]]; then
  JAVA_RUN=$JAVA_HOME/bin/java
else
  JAVA_RUN=java
fi

# Define HOSTNAME if it is not already set
if [ -z "${HOSTNAME}" ]; then
    HOSTNAME=`hostname`
fi

IS_NUMBER="^[0-9]+$"

if [ -z "${PSERVER_PID_DIR}" ]; then
    PSERVER_PID_DIR=$(readFromConfig ${KEY_ENV_PID_DIR} "${DEFAULT_ENV_PID_DIR}" "${YAML_CONF}")
fi

if [ -z "${PSERVER_ARCHIVE_DIR}" ]; then
    PSERVER_ARCHIVE_DIR=$(readFromConfig ${KEY_ENV_ARCHIVE_DIR} "${DEFAULT_ENV_ARCHIVE_DIR}" "${YAML_CONF}")
    if [ "${PSERVER_ARCHIVE_DIR}" = "" ]; then
      echo "env.archive.dir in /conf/pserver-conf.yaml is not set! [required], was: ${PSERVER_ARCHIVE_DIR}"
      exit 1
    fi
    PSERVER_NODE_DEPLOY_DIR=${PSERVER_ARCHIVE_DIR}/${ARCHIVE}
fi

if [ -z "${PSERVER_ENV_JAVA_OPS}" ]; then
    PSERVER_ENV_JAVA_OPS=$(readFromConfig ${KEY_ENV_JAVA_OPTS} "${DEFAULT_ENV_JAVA_OPTS}" "${YAML_CONF}")

    # Remove leading and ending double quotes (if present) of value
    PSERVER_ENV_JAVA_OPS="$( echo "${PSERVER_ENV_JAVA_OPS}" | sed -e 's/^"//'  -e 's/"$//' )"
fi

if [ -z "${PSERVER_SSH_OPTS}" ]; then
    PSERVER_SSH_OPTS=$(readFromConfig ${KEY_ENV_SSH_OPTS} "${DEFAULT_ENV_SSH_OPTS}" "${YAML_CONF}")
fi

if [ -z "${PSERVER_PROFILE}" ]; then
    PSERVER_PROFILE=$(readFromConfig ${KEY_ENV_PROFILE} "${DEFAULT_ENV_PROFILE}" "${YAML_CONF}")
fi

if [ -z "${PSERVER_NODE_HEAP}" ]; then
    PSERVER_NODE_HEAP=$(readFromConfig ${KEY_ENV_NODE_HEAP_MB} "${DEFAULT_NODE_HEAP}" "${YAML_CONF}")
fi
# Arguments for the JVM. Used for job and task manager JVMs.
# DO NOT USE FOR MEMORY SETTINGS!
JVM_ARGS=""
