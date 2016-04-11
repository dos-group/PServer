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

# cloud-7 has fix host list, so no need for customization
#
# function profile_parse_arg() { }
#
# function check_args_for_slaves_gen() { }
# function gen_slaves_hostlist() { }
# function check_args_for_zookeepers_gen() { }
# function gen_zookeeper_hostlist() { }
#
function profile_env() {
	PARAM_COUNT_OF_ZOOKEEPER_NODES=3
}

function profile_finish_hook() {
	if [ $FILE == "staged-cluster" ] && [ $CMD == "pserver-stage" ]; then
		ssh -n $STAGING_SSH_OPTS ${PSERVER_STAGING_HOST} -- "chmod -R a+w ${PSERVER_STAGING_DIRECTORY}"
	fi
}