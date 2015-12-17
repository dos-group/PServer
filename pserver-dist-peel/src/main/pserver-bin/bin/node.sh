#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config-cluster.sh

if [ ! -z ${1+x} ] && [ -z ${CMD+x} ]; then
    CMD=$1
    shift
fi

case ${CMD} in

start)

PSERVER_NODE_CLASSPATH=`constructClassPath`

log=$PSERVER_LOG_DIR/pserver-node-$HOSTNAME.log
out=$PSERVER_LOG_DIR/pserver-node-$HOSTNAME.out
pid=$PSERVER_PID_DIR/pserver-node.pid
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file:"$PSERVER_CONF_DIR"/log4j.properties"

JVM_ARGS="$JVM_ARGS -XX:+UseParNewGC -XX:NewRatio=8 -XX:PretenureSizeThreshold=64m -Xms"$PSERVER_NODE_HEAP"m -Xmx"$PSERVER_NODE_HEAP"m"

mkdir -p "$PSERVER_PID_DIR"
        if [ -f $pid ]; then
            if kill -0 `cat $pid` > /dev/null 2>&1; then
                echo "[NOTICE][$(hostname)] pserver node running as process `cat $pid`.  Stop it first."
                exit 1
            fi
        fi

echo "[NOTICE][$(hostname)] Starting pserver node ... "
$JAVA_RUN $JVM_ARGS $PSERVER_OPTS $log_setting -classpath $PSERVER_NODE_CLASSPATH de.tuberlin.pserver.node.PServerMain --config-dir=$PSERVER_CONF_DIR --profile=$PSERVER_PROFILE > "$out" 2>&1 < /dev/null &
echo $! > $pid
;;

stop)

pid=${PSERVER_PID_DIR}/pserver-node.pid
        if [ -f $pid ]; then
            if kill -0 `cat $pid` > /dev/null 2>&1; then
                echo "[NOTICE][${HOSTNAME}] Stopping pserver node ..."
                kill `cat $pid`
                rm "${PSERVER_PID_DIR}/pserver-node.pid"
            else
                >&2 echo "[NOTICE][${HOSTNAME}] pserver is not running. "
            fi
        else
            >&2 echo "[NOTICE][${HOSTNAME}] pid file of pserver not found. Nothing to do. "
        fi
        ;;

*)

echo "Node only supports start or stop - wrong command: $CMD"
;;

esac
