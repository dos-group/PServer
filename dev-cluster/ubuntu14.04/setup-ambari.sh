#!/usr/bin/env bash

#su - postgres

# Add repo and install PostgreSQL and ambari-server
#sudo apt-get autoremove -y
sudo wget -O /etc/apt/sources.list.d/ambari.list http://public-repo-1.hortonworks.com/ambari/ubuntu14/2.x/updates/2.2.1.0/ambari.list
sudo apt-key adv --recv-keys --keyserver keyserver.ubuntu.com B9733A7A07513CAD
sudo apt-get update
sudo apt-get install postgresql postgresql-contrib -y
sudo -u postgres psql -c "create database ambari;"
sudo -u postgres psql -c "create database ambarirca;"
sudo service postgresql restart
#sudo apt-get install ambari-server -y

# Setup and start ambari-server
#sudo ambari-server setup -s -v -j /usr/lib/jvm/java-8-oracle
#sudo ambari-server setup -s
#sudo ambari-server start
