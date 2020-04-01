# Clustering Module

Clustering Module performs segmentation analysis on Docker container performance data as a part of an container management system. Provides a summary information for given columns (i.e. CPU usage percent, RAM usage, network usage).

Clustering Module helps sysadmin to observe system resource usage and utilization trends for Docker containers. Module first seperates data into macro and micro segments, and after that runs clustering algorithms on that segmented data.

Clustering Module uses [Apache Spark™](https://spark.apache.org/) for clustering process, [Apache™ Hadoop®](https://hadoop.apache.org/) for reading data from,[MongoDB](https://www.mongodb.com/) for storing configurations and data and [Django](https://www.djangoproject.com/), [pandas](https://pandas.pydata.org/), [Matplotlib](https://matplotlib.org/), [D3.JS](https://d3js.org/) for visualizing data.



**<u>Commands below are for Ubuntu 18.04 LTS, please change this commands for your operating system!</u>**

## Docker

This section document is for running clustering module on Docker containers. *You can skip this part and go directly Local section for running clustering module on bare metal or on a virtual machine.*



### Setup

Install Docker

```bash
sudo apt-get remove docker docker-engine docker.io containerd runc
sudo apt-get update

sudo apt-get install \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common
    
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo apt-key fingerprint 0EBFCD88

sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
   
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io

sudo usermod -aG docker your-user
# Change your-user with username
```



Copy (or download, git clone) module archive here and extract archive

```bash
# Clone repository
git clone https://github.com/ykursadkaya/Clustering/
# or
# Download or copy archive
tar xzvf Clustering.tar.gz (filename can be change)

cd Clustering
```



Change MONGO_URL in clustering-Dockerfile and optimalk-Dockerfile

```bash
# Open these files with your text editor
nano clustering-Dockerfile
nano optimalk-Dockerfile
# Find "ENV MONGO_URL mongodb://clustering_mongodb:27017/" line
# And change MongoDB URL
# Example MongoDB URL -> mongodb://172.19.0.2:27017/
# Save file
```



### Run

If you want to create containers and add cronjobs to run containers periodically just run setup.sh file

```bash
chmod 777 setup.sh
./setup.sh
# If your Hadoop installation or MongoDB server is on same Docker cluster and connected by a network
# You must connect all clustering module containers to this network
# i.e Docker network name is clustering-net
# You can either give another argument while creating container
# Example: docker create --name daily_clustering --network clustering-net clustering daily
# (Recommended) Or you can connect container to a network afterwards (other option requires editing setup.sh file)
# Example: docker network connect clustering-net daily_clustering
```



#### *Running manually*

If you want to run containers by yourself. *You can skip this part if you want to just setup Clustering Module with cronjobs*



##### Setup

```bash
# Pull Python and OpenJDK images
docker pull python:3.7.7-slim-buster
docker pull openjdk:8-slim-buster

# Build clustering and optimalk images
docker build -t optimalk -f optimalk-Dockerfile .
docker build -t clustering -f clustering-Dockerfile .

# Create containers
docker create --name optimal_k optimalk

docker create --name daily_clustering clustering daily
docker create --name weekly_clustering clustering weekly
docker create --name monthly_clustering clustering monthly
docker create --name yearly_clustering clustering yearly

# If your Hadoop installation or MongoDB server is on same Docker cluster
# i.e Docker network name is clustering-net
# You can either give another argument while creating container
# Example: docker create --name daily_clustering --network clustering-net clustering daily
# Or you can connect container to a network afterwards
# Example docker network connect clustering-net daily_clustering
```



##### Run

```bash
# Then you can start the containers
docker start optimal_k
# You must run optimal_k container first and wait for to finish its job
docker start daily_clustering
```



## Local

This section document is for running clustering module on bare metal or on a virtual machine. *You can skip this part and go directly Docker section for running clustering module on Docker containers.*

### Setup

Create a user group and a user both named "clustering"

```bash
sudo useradd -rm -d /home/clustering -s /bin/bash -g root -G sudo -u 1000 clustering
```

Switch to "clustering" user

```bash
sudo su clustering
```

Change directory to /home/clustering, copy (or download, git clone) module archive here and extract archive

```bash
cd /home/clustering

# Clone repository
git clone https://github.com/ykursadkaya/Clustering/
# or
# Download or copy archive
tar xzvf Clustering.tar.gz (filename can be change)

cd Clustering
```



Update the apt repository

```bash
sudo apt update
```



Install Python 3.7.x (or 3.6.x depends on Ubuntu version 18.04 default repository version is 3.6) and OpenJDK 8

```bash
sudo apt install -y python3 python3-pip openjdk-8-jdk-headless

```

Install required Python libraries

```bash
sudo pip3 install -r requirements.txt
```



Go to parent folder that contains Clustering Module

```bash
cd ~/Clustering
```

Add MongoDB URL to .bashrc file (open ~/.bashrc file with your text editor)

```bash
nano ~/.bashrc
# Add "export MONGO_URL=mongodb://(mongodb-ip):(mongodb-port)/" line to end of the file
# Example MongoDB URL -> mongodb://172.19.0.2:27017/
# Save file
source ~/.bashrc
```



Run add_conf.py file to insert example configuration on MongoDB

```
python3 add_conf.py
```

**Then change the example configuration with your own configuration on MongoDB!**



#### *On Hadoop machine*

*Go to the machine which Hadoop (HDFS) is located and create a user group and a user both named "clustering", also add your Hadoop user to "clustering" group*

```bash
groupadd -r clustering && useradd --no-log-init -r -g clustering clustering
usermod -a -G clustering root
```

*Create /user/clustering directory on HDFS and change owner of this folder to clustering:clustering*

```bash
hdfs dfs -mkdir /
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/clustering

hdfs dfs -chown -R clustering:clustering /user/clustering
```





### Run

Add cronjobs to crontab

```bash
(crontab -l 2>/dev/null; echo "30 0 1 * * python3 /home/clustering/Clustering/scripts/optimal_k.py") | crontab -

(crontab -l 2>/dev/null; echo "30 0 * * * python3 /home/clustering/Clustering/scripts/clustering.py daily") | crontab -

(crontab -l 2>/dev/null; echo "30 0 * * 0 python3 /home/clustering/Clustering/scripts/clustering.py weekly") | crontab -

(crontab -l 2>/dev/null; echo "30 0 1 * * python3 /home/clustering/Clustering/scripts/clustering.py monthly") | crontab -

(crontab -l 2>/dev/null; echo "30 0 1 1 * python3 /home/clustering/Clustering/scripts/clustering.py yearly") | crontab -
```





#### *Running manually*

If you want to run scripts by yourself. *You can skip this part if you want to just setup Clustering Module with cronjobs*



You must run optimal_k.py script first and wait for finish its job.

```bash
python3 /home/clustering/Clustering/scripts/optimal_k.py

python3 /home/clustering/Clustering/scripts/clustering.py daily
```

