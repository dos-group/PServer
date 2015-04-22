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

STARTSTOP=$1

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/env.sh

if [ "$PSERVER_IDENT_STRING" = "" ]; then
    PSERVER_IDENT_STRING="$USER"
fi

# auxilliary functionTypeName to construct a lightweight classpath for a
# pserver node
constructNodeClassPath() {

    for jarfile in $PSERVER_LIB_DIR/*.jar ; do
        if [[ $PSERVER_NODE_CLASSPATH = "" ]]; then
            PSERVER_NODE_CLASSPATH=$jarfile;
        else
            PSERVER_NODE_CLASSPATH=$PSERVER_NODE_CLASSPATH:$jarfile
        fi
    done

    echo $PSERVER_NODE_CLASSPATH
}

PSERVER_NODE_CLASSPATH=`manglePathList $(constructNodeClassPath)`

log=$PSERVER_LOG_DIR/pserver-$PSERVER_IDENT_STRING-node-$HOSTNAME.log
out=$PSERVER_LOG_DIR/pserver-$PSERVER_IDENT_STRING-node-$HOSTNAME.out
pid=$PSERVER_PID_DIR/pserver-$PSERVER_IDENT_STRING-node.pid
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file:"$PSERVER_CONF_DIR"/log4j.properties"

JVM_ARGS="$JVM_ARGS -XX:+UseParNewGC -XX:NewRatio=8 -XX:PretenureSizeThreshold=64m -Xms"$PSERVER_NODE_HEAP"m -Xmx"$PSERVER_NODE_HEAP"m"

case $STARTSTOP in

    (start)
        mkdir -p "$PSERVER_PID_DIR"
        if [ -f $pid ]; then
            if kill -0 `cat $pid` > /dev/null 2>&1; then
                echo pserver node running as process `cat $pid` on host $HOSTNAME.  Stop it first.
                exit 1
            fi
        fi

        # Rotate log files
        rotateLogFile $log
        rotateLogFile $out

        echo Starting pserver node on host $HOSTNAME
        $JAVA_RUN $JVM_ARGS $PSERVER_OPTS $log_setting -classpath $PSERVER_NODE_CLASSPATH de.tuberlin.pserver.node.PServerMain --config-dir=$PSERVER_CONF_DIR > "$out" 2>&1 < /dev/null &
        echo $! > $pid
    ;;

    (stop)
        if [ -f $pid ]; then
            if kill -0 `cat $pid` > /dev/null 2>&1; then
                echo Stopping pserver node on host $HOSTNAME
                kill `cat $pid`
            else
                echo No pserver node to stop on host $HOSTNAME
            fi
        else
            echo No pserver node to stop on host $HOSTNAME
        fi
    ;;

    (*)
        echo Please specify start or stop
    ;;

esac
