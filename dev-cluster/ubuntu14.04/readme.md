
**PServer Development Cluster Environment**
***************************************

This will guide you through the process of setting up a local development cluster consisting of up to 10 Ubuntu 14.04 virtual machines as well as how to manage distributed services using Apache Ambari.


**Prerequisites**
*************

You will need a conneciton to the internet.

The following software needs to be installed locally before you can initialize the dev-cluster.

 1. VirtualBox (https://www.virtualbox.org/wiki/Linux_Downloads)

 2. Vagrant (https://www.vagrantup.com/docs/installation/)

Copy the contents of the hosts file (excluding the first two lines) into your /etc/hosts file. This will require sudo access.


**Installing the cluster** 
**********************

Apache Ambari is a tool for provisioning, managing, and monitoring Apache Hadoop clusters. Ambari consists of a set of RESTful APIs and a browser-based management interface. 

To change the setup (e.g. memory) of virtual mahcines, edit the "Vagrantfile".

To initialize the cluster, run "./up.sh num_of_nodes" with num_of_nodes being an integer between 1 and 10 which represents the number of nodes you want in your cluster.

This will setup however many nodes you requested with Ubuntu 14.04 and install Ambari Server v2.2.1 on u1401. It will take approximately 15 minutes (For 3 nodes and assuming all steps are successful)

For troubleshooting see https://cwiki.apache.org/confluence/display/AMBARI/Quick+Start+Guide


**Setting up the Services**
***********************

After the cluster has successfully been installed, the next task is to setup the services for our cluster. Open the ambari management console at http://c7101.ambari.apache.org:8080, the login username and password is "admin".

Click "launch install wizard" and proceed to install the services you would like on your cluster. 

When you get to "Install Options" you can enter the following into "Target Hosts":

u14[01-03].ambari.apache.org

NOTE: this assumes you have 3 nodes, if you have more/less, just change the values in brackets.

Also on "Install Options", under "Host Registration Information" you can find the SSH private key stored in a file in the same directory as the rest of the dev-cluster files called "insecure_private_key".

Once you get to the end of the install wizard, congratulations on your new development cluster!

To stop the cluster you can use "vagrant suspend" in the console. To bring it back up again, "./up.sh num_of_nodes". Good luck!


**Basic VM Operations**
*******************

vagrant up <vm name>
Starts a specific VM. up.sh is a wrapper for this call.
Note: if you don’t specify the <vm name> parameter, it will try to start 10 VMs 
You can run this if you want to start more VMs after you already called up.sh
For example: vagrant up u1406

vagrant destroy -f
Destroys all VMs launched from the current directory (deletes them from disk as well)
You can optionally specify a specific VM to destroy

vagrant suspend
Suspends (snapshot) all VMs launched from the current directory so that you can resume them later
You can optionally specify a specific VM to suspend

vagrant resume
Resumes all suspended VMs launched from the current directory
You can optionally specify a specific VM to resume

vagrant ssh host
Starts a SSH session to the host. For example: vagrant ssh u1401

vagrant status
Shows which VMs are running, suspended, etc.


**HDFS Commands**
*************

Making a new Directory
HADOOP_USER_NAME=hdfs hdfs dfs -mkdir /datasets

Putting all files in a directory to HDFS
HADOOP_USER_NAME=hdfs hdfs dfs -put /vagrant/datasets /

View files in a folder
HADOOP_USER_NAME=hdfs hdfs dfs -ls /datasets

Delete a directory
HADOOP_USER_NAME=hdfs hdfs dfs -rmr /datasets

