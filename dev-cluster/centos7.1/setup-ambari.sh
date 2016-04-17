#!/usr/bin/env bash

# Install ambari-server
wget -O /etc/yum.repos.d/ambari.repo http://public-repo-1.hortonworks.com/ambari/centos7/2.x/updates/2.2.1.0/ambari.repo
sudo yum install ambari-server -y

# Setup and start ambari-server
sudo ambari-server setup -svj /usr/java/jdk1.8.0_77
sudo ambari-server start
