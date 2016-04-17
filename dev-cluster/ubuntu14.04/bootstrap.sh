#!/usr/bin/env bash
cp /vagrant/hosts /etc/hosts
cp /vagrant/resolv.conf /etc/resolv.conf
mkdir -p /root/.ssh; chmod 600 /root/.ssh; cp /home/vagrant/.ssh/authorized_keys /root/.ssh/

# Increasing swap space
dd if=/dev/zero of=/swapfile bs=1024 count=3072k
mkswap /swapfile
swapon /swapfile
echo "/swapfile       none    swap    sw      0       0" >> /etc/fstab

cp /vagrant/insecure_private_key /root/insecure_private_key
chmod 600 /root/insecure_private_key

# wget and install Java 8
sudo apt-get install -y software-properties-common python-software-properties debconf-utils
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get update
echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | sudo debconf-set-selections
sudo apt-get install -y oracle-java8-installer

# Dependancies for parameter server and petuum
sudo apt-get -y install g++ make autoconf git libtool uuid-dev openssh-server cmake libopenmpi-dev openmpi-bin libssl-dev libnuma-dev python-dev python-numpy python-scipy python-yaml protobuf-compiler subversion libxml2-dev libxslt-dev zlibc zlib1g zlib1g-dev libbz2-1.0 libbz2-dev libboost-all-dev
