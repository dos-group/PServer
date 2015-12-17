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

echo "[NOTICE][$(hostname)] Stopping PServer on $(wc -l $HOSTLIST | sed -e 's/\s.*$//') nodes..."

while IFS= read HOST
do
  echo "[NOTICE][$(hostname)] Stopping node on ${HOST} ... "
  ssh -n $HOST -- "nohup /bin/bash ${PSERVER_BIN_DIR}/node.sh stop &"
  # use this line for cluster setups without shared home
  # ssh -n $HOST -- "nohup /bin/bash ${PSERVER_NODE_DEPLOY_DIR}/bin/node.sh stop &"
done <"$HOSTLIST"

# update status
${PSERVER_BIN_DIR}/status.sh
