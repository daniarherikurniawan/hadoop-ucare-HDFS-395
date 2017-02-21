#!/bin/bash

sudo mkdir -p /mnt/extra /tmp/hadoop-ucare
sudo chown -R $USER:ucare /mnt/extra /tmp/hadoop-ucare
sudo chmod -R 775 /mnt/extra /tmp/hadoop-ucare

cd /mnt/extra
mkdir hadoop
cd hadoop
git init

git remote add dan-github https://github.com/daniarherikurniawan/hadoop-ucare-HDFS-395.git
git pull dan-github origin master
git checkout origin master

# run command below for initial compilation
# mvn package install -Pdist -DskipTests
