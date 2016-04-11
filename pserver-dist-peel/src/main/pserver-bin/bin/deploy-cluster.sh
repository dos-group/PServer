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

# Create archive from this version of the pserver
echo "[NOTICE][$(hostname)] Creating archive: $ARCHIVE_NAME from $ARCHIVE_DIR"
`cd "$ARCHIVE_DIR"; tar czf ${ARCHIVE}.tar.gz ${ARCHIVE}`

# copy pserver archive to every node and extract
while IFS= read HOST
do
  echo "[NOTICE][$(hostname)] Copying archive to: ${HOST}:${PSERVER_ARCHIVE_DIR} ... "

  ssh -n $HOST -- "rm -rf ${PSERVER_ARCHIVE_DIR}/${ARCHIVE}; mkdir -p $PSERVER_ARCHIVE_DIR"
  scp "${ARCHIVE_PATH}" "${HOST}:${PSERVER_ARCHIVE_DIR}/"
  ssh -n $HOST -- "cd $PSERVER_ARCHIVE_DIR; tar -xzf ${ARCHIVE}.tar.gz"
done <"$HOSTLIST"

echo "[SUCCESS][$(hostname)] PServer deployment completed at ${PSERVER_NODE_DEPLOY_DIR} !"
