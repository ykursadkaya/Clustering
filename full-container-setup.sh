#!/bin/bash

CRON_MINUTE=30
CRON_HOUR=0

pull_images ()
{
	docker pull python:3.7.7-slim-buster
	docker pull openjdk:8-slim-buster
	docker pull mongo:4.2.3
	docker pull sequenceiq/hadoop-docker:2.7.0
}


build_images ()
{
	docker build -t optimalk -f optimalk-Dockerfile .
	docker build -t clustering -f clustering-Dockerfile .
	docker build -t hdfs-clustering -f HDFS/hadoop-Dockerfile .
}


create_network ()
{
	docker network create --driver bridge clustering-net
}


create_containers ()
{
	docker create -p 27017-27019:27017-27019 --name clustering_mongodb --network clustering-net mongo:4.2.3
	docker create --name clustering_hdfs --network clustering-net -it hdfs-clustering /etc/bootstrap.sh -bash

	docker create --name optimal_k --network clustering-net optimalk

	docker create --name daily_clustering --network clustering-net clustering daily
	docker create --name weekly_clustering --network clustering-net clustering weekly
	docker create --name monthly_clustering --network clustering-net clustering monthly
	docker create --name yearly_clustering --network clustering-net clustering yearly
}


start_db ()
{
	if [ ! "$(docker ps -qa -f name=clustering_mongodb)" ]; then
		docker run -p 27017-27019:27017-27019 --name clustering_mongodb --network clustering-net mongo:4.2.3
	else
		docker start clustering_mongodb
	fi
}


start_hdfs ()
{
	if [ ! "$(docker ps -qa -f name=clustering_hdfs)" ]; then
		docker run --name clustering_hdfs --network clustering-net -it hdfs-clustering /etc/bootstrap.sh -bash
	else
		docker start clustering_hdfs
	fi
}


start_clustering ()
{
	docker start clustering $1
}


start_optimalk ()
{
	docker start optimal_k
}


run_containers ()
{
	clustering_type=$1
	container_name="${clustering_type}_clustering"

	if [ ! "$(docker ps -qa -f name=${container_name})" ]; then
		docker run --name $container_name --network clustering-net clustering $clustering_type
	else
		start_clustering $container_name
	fi
}


add_crontab ()
{
	(crontab -l 2>/dev/null; echo "${CRON_MINUTE} ${CRON_HOUR} * * * if [ ! docker ps -qa -f name=daily_clustering ]; then docker run --name daily_clustering clustering daily; else docker start daily_clustering; fi") | crontab -
	(crontab -l 2>/dev/null; echo "${CRON_MINUTE} ${CRON_HOUR} * * 0 if [ ! docker ps -qa -f name=weekly_clustering ]; then docker run --name weekly_clustering clustering weekly; else docker start weekly_clustering; fi") | crontab -
	(crontab -l 2>/dev/null; echo "${CRON_MINUTE} ${CRON_HOUR} 1 * * if [ ! docker ps -qa -f name=monthly_clustering ]; then docker run --name monthly_clustering clustering monthly; else docker start monthly_clustering; fi") | crontab -
	(crontab -l 2>/dev/null; echo "${CRON_MINUTE} ${CRON_HOUR} 1 1 * if [ ! docker ps -qa -f name=yearly_clustering ]; then docker run --name yearly_clustering clustering yearly; else docker start yearly_clustering; fi") | crontab -
	(crontab -l 2>/dev/null; echo "${CRON_MINUTE} ${CRON_HOUR} 1 * * if [ ! docker ps -qa -f name=optimal_k ]; then docker run --name optimal_k optimalk; else docker start optimal_k; fi") | crontab -
}

remove_containers ()
{
	docker rm daily_clustering weekly_clustering monthly_clustering yearly_clustering optimal_k
}

remove_db_hdfs ()
{
	docker rm clustering_hdfs clustering_mongodb
}


remove_containers
remove_db_hdfs
pull_images
build_images
# start_db
# start_hdfs
create_network
create_containers
# start_optimalk
# start_clustering daily
# add_crontab
