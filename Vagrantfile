# -*- mode: ruby -*-
# vi: set ft=ruby :

# INSTALL ALL THE THINGS
$setup = <<SCRIPT
set -e

# Setup
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF
DISTRO=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
CODENAME=$(lsb_release -cs)

# Add the Mesos repository
echo "deb http://repos.mesosphere.io/${DISTRO} ${CODENAME} main" | \
  sudo tee /etc/apt/sources.list.d/mesosphere.list

# Update package list and install Mesos/JDK/Maven
sudo apt-get -y update
sudo apt-get -y install mesos=1.1.0-2.0.107.debian81 maven default-jdk git apt-transport-https

# Set Mesos IP/containerizer instance
sudo bash -c "echo 192.168.33.50 > /etc/mesos-master/ip"
sudo bash -c "echo 192.168.33.50 > /etc/mesos-slave/ip"
sudo bash -c "echo mesos > /etc/mesos-slave/containerizers"

# Start a bunch of services
sudo service zookeeper restart
sleep 5
(sudo service mesos-master stop || true)
(sudo service mesos-slave stop || true)

# Download and extract Hadoop
wget http://archive.cloudera.com/cdh5/cdh/5/hadoop-2.5.0-cdh5.2.0.tar.gz
tar -zxvf hadoop-2.5.0-cdh5.2.0.tar.gz
sudo mkdir -p /usr/local/hadoop-2.5.0
sudo chmod +w /usr/local/hadoop-2.5.0
mv hadoop-2.5.0-cdh5.2.0/* /usr/local/hadoop-2.5.0
rm hadoop-2.5.0-cdh5.2.0.tar.gz

# copy across .bash_profile to set environment
ln -s /opt/mesos-hadoop/vagrant/bash/.bash_profile /home/vagrant/.bash_profile

# setup ssh key for SSHing to localhost
ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa
cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys

# Update symlinks etc to point to MR1
cd /usr/local/hadoop-2.5.0

mv bin bin-mapreduce2
mv examples examples-mapreduce2
ln -s bin-mapreduce1 bin
ln -s examples-mapreduce1 examples

pushd etc > /dev/null
mv hadoop hadoop-mapreduce2
ln -s hadoop-mapreduce1 hadoop
popd > /dev/null

pushd share/hadoop > /dev/null
rm mapreduce
ln -s mapreduce1 mapreduce
popd > /dev/null

sudo mkdir -p /var/log/hadoop
sudo chown vagrant /var/log/hadoop

# ln hadoop conf files to appropriate locations
pushd /usr/local/hadoop-2.5.0 > /dev/null
sudo chown vagrant etc/hadoop

rm etc/hadoop/core-site.xml
rm etc/hadoop/hdfs-site.xml
rm etc/hadoop/mapred-site.xml

ln -s /opt/mesos-hadoop/vagrant/hadoop/core-site.xml etc/hadoop/core-site.xml
ln -s /opt/mesos-hadoop/vagrant/hadoop/hdfs-site.xml etc/hadoop/hdfs-site.xml
ln -s /opt/mesos-hadoop/vagrant/hadoop/mapred-site.xml etc/hadoop/mapred-site.xml
popd > /dev/null

# build mesos-hadoop and link it to the appropriate directory in hadoopland
cd /opt/mesos-hadoop
mvn package
# TODO can this be version independent? can this be automated?
cp target/hadoop-mesos-0.2.0.jar /usr/local/hadoop-2.5.0/share/hadoop/common/lib/hadoop-0.2.0.jar

source /opt/mesos-hadoop/vagrant/bash/.bash_profile

pushd /usr/local/hadoop-2.5.0/bin > /dev/null
# launch namenode/datanode (50070/50075)
./hadoop namenode -format
./hadoop-daemon.sh start namenode
./hadoop-daemon.sh start datanode

# TODO how to make this repeatable?
pushd /usr/local/hadoop-2.5.0 > /dev/null
sudo tar -czf /opt/mesos-hadoop/hadoop-2.5.0-cdh5.2.0.tar.gz .
popd > /dev/null
./hadoop fs -put /opt/mesos-hadoop/hadoop-2.5.0-cdh5.2.0.tar.gz /hadoop-2.5.0-cdh5.2.0.tar.gz

# launch jobtracker (50030)
./hadoop-daemon.sh start jobtracker
popd > /dev/null

# download cloudera install
wget http://archive.cloudera.com/cdh5/one-click-install/precise/amd64/cdh5-repository_1.0_all.deb
sudo dpkg -i cdh5-repository_1.0_all.deb
rm cdh5-repository_1.0_all.deb
sudo apt-get update
sudo apt-get install -y hadoop-client

# Start mesos
sudo service mesos-master start
sudo service mesos-slave start

SCRIPT

Vagrant.configure("2") do |config|

  config.vm.box = "puppetlabs/debian-8.2-64-nocm"

  config.vm.synced_folder "./", "/opt/mesos-hadoop"
  config.vm.network :private_network, ip: "192.168.33.50"

  # Configure the VM with 4048Mb of RAM and 2 CPUs
  config.vm.provider :virtualbox do |vb|
    vb.customize ["modifyvm", :id, "--memory", "4048"]
    vb.customize ["modifyvm", :id, "--cpus", "2"]
  end

  # Add the configured key into the ssh agent.
  config.trigger.before [:provision, :up, :reload, :ssh] do
    path = File.dirname(File.realpath(__FILE__))
    system("ssh-add -l || ssh-add #{path}/vagrant/ssh/id_[rd]sa")
  end
  config.ssh.forward_agent = true

  # ensure VM clock stays in sync with correct time zone
  config.vm.provision :shell, :inline => "sudo rm /etc/localtime && sudo ln -s /usr/share/zoneinfo/Europe/London /etc/localtime", run: "always"

  # Install all the things!
  config.vm.provision "shell", inline: $setup
end
