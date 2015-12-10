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

echo "[NOTICE][$(hostname)] Starting PServer on $(wc -l $HOSTLIST | sed -e 's/\s.*$//') nodes..."

# deploy binaries at every node if there is no shared home directory on the cluster
# ${PSERVER_BIN_DIR}/deploy-cluster.sh

while IFS= read HOST
do
  echo "[NOTICE][$(hostname)] Starting node on ${HOST} ... "
  ssh -n $HOST -- "nohup /bin/bash ${PSERVER_BIN_DIR}/node.sh start &"
  # use this line for clusters without shared home directory
  # ssh -n $HOST -- "nohup /bin/bash ${PSERVER_NODE_DEPLOY_DIR}/bin/node.sh start &"
done <"$HOSTLIST"

# collect status info
${PSERVER_BIN_DIR}/status.sh
