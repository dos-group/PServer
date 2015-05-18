Quick Start Guide (With wally profile)

This guide is intended for users wanting to quickly run the pserver on the wally cluster.

If you have direct access to the nodes of the cluster, use the commands as they are named in the following.
But if you only have access to the master node, prepent "staged-" to all script names. Example:
sbin/cluster.sh fetch-logs -f <first-wally-node-id> -c <number-of-nodes-to-use> -fetch-logs-interval <in seconds>
sbin/cluster-staged.sh fetch-logs -f <first-wally-node-id> -c <number-of-nodes-to-use> -fetch-logs-interval <in seconds>

0. Make sure your project is built with mvn package
1. Add all runtime configuration parameters like heap-space, java-home, ssh-options etc. to conf/pserver.conf
2. Add all deployment specific options like target-directories etc. to conf/deploy.conf
3. Run scripts/deploy-zookeeper-setup-start-all.sh -f <first-wally-node-id> -c <number-of-nodes-to-use> -z <number-of-zookeeper-nodes>
   This will do the following:
   - upload the built binary package (pserver-dist/target) to the specified range of wally node
   - install and configure zookeeper in the specified range
   - start zookeeper- and pserver-nodes
4. Run sbin/cluster.sh fetch-logs -f <first-wally-node-id> -c <number-of-nodes-to-use> -fetch-logs-interval <in seconds>
   This will repeatedly gather all available logs from the specified node range and copy them into the log directory.
   You can monitor the log files with "less -F".
5. When you are done, run scripts/stop-all-fetch-logs.sh -f <first-wally-node-id> -c <number-of-nodes-to-use> -z <number-of-zookeeper-nodes>
   This will stop all running nodes and fetch the final