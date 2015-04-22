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

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/env.sh

HOSTLIST=$PSERVER_SLAVES

if [ "$HOSTLIST" = "" ]; then
    HOSTLIST="${PSERVER_CONF_DIR}/slaves"
fi

if [ ! -f $HOSTLIST ]; then
    echo $HOSTLIST is not a valid slave list
    exit 1
fi

GOON=true
while $GOON
do
    read HOST || GOON=false
    if [ -n "$HOST" ]; then
        ssh -n $PSERVER_SSH_OPTS $HOST -- "nohup /bin/bash $PSERVER_BIN_DIR/node.sh stop &"
    fi
done < $HOSTLIST
