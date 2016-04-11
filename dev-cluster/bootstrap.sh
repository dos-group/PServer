#!/usr/bin/env bash

cp /vagrant/hosts /etc/hosts
cp /vagrant/resolv.conf /etc/resolv.conf
yum install ntp -y
sudo service ntpd start
sudo service iptables stop
mkdir -p /root/.ssh; chmod 600 /root/.ssh; cp /home/vagrant/.ssh/authorized_keys /root/.ssh/

# Again, stopping iptables
/etc/init.d/iptables stop

# Turn off firewall on boot
sudo chkconfig iptables off

# Increasing swap space
sudo dd if=/dev/zero of=/swapfile bs=1024 count=3072k
sudo mkswap /swapfile
sudo swapon /swapfile
echo "/swapfile       none    swap    sw      0       0" >> /etc/fstab

sudo cp /vagrant/insecure_private_key /root/ec2-keypair
sudo chmod 600 /root/ec2-keypair

# wget and install Java 8
wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u77-b03/jdk-8u77-linux-x64.rpm -O jdk-8u77-linux-x64.rpm
#sudo rpm -ivh /vagrant/software/jdk-8u77-linux-x64.rpm
sudo rpm -ivh jdk-8u77-linux-x64.rpm
sudo rm -rf jdk-8u77-linux-x64.rpm
