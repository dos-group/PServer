#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config-cluster.sh

if [ -z ${HOSTLIST+x} ]; then
    HOSTLIST="${PSERVER_CONF_DIR}/nodes"
fi

if [ ! -f "$HOSTLIST" ]; then
    echo $HOSTLIST is not a valid list of nodes
    exit 1
fi

STATUS_FILE=${PSERVER_LOG_DIR}/status.txt

# make sure we reset the status file
if [ -f $STATUS_FILE ]; then
  rm $STATUS_FILE
fi

touch $STATUS_FILE

# collect status information from every node

while IFS= read HOST; do
  STATUS=$(ssh -n $HOST -- "jps | grep PServerMain")

  if [ ! -z "$STATUS" ]; then 
    echo "Node on $HOST is up and running!" >> $STATUS_FILE
  fi
done <"$HOSTLIST"


