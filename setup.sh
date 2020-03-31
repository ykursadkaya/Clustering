#!/bin/bash

CRON_MINUTE=30
CRON_HOUR=0

pull_images ()
{
	docker pull python:3.7.7-slim-buster
	docker pull openjdk:8-slim-buster
}


build_images ()
{
	docker build -t optimalk -f optimalk-Dockerfile .
	docker build -t clustering -f clustering-Dockerfile .
}


create_network ()
{
	docker network create --driver bridge clustering-net
}


create_containers ()
{
	docker create --name optimal_k optimalk

	docker create --name daily_clustering clustering daily
	docker create --name weekly_clustering clustering weekly
	docker create --name monthly_clustering clustering monthly
	docker create --name yearly_clustering clustering yearly
}


add_crontab ()
{
	(crontab -l 2>/dev/null; echo "${CRON_MINUTE} ${CRON_HOUR} * * * if [ ! "$(docker ps -qa -f name=daily_clustering)" ]; then docker run --name daily_clustering clustering daily; else docker start daily_clustering; fi") | crontab -
	(crontab -l 2>/dev/null; echo "${CRON_MINUTE} ${CRON_HOUR} * * 0 if [ ! "$(docker ps -qa -f name=weekly_clustering)" ]; then docker run --name weekly_clustering clustering weekly; else docker start weekly_clustering; fi") | crontab -
	(crontab -l 2>/dev/null; echo "${CRON_MINUTE} ${CRON_HOUR} 1 * * if [ ! "$(docker ps -qa -f name=monthly_clustering)" ]; then docker run --name monthly_clustering clustering monthly; else docker start monthly_clustering; fi") | crontab -
	(crontab -l 2>/dev/null; echo "${CRON_MINUTE} ${CRON_HOUR} 1 1 * if [ ! "$(docker ps -qa -f name=yearly_clustering)" ]; then docker run --name yearly_clustering clustering yearly; else docker start yearly_clustering; fi") | crontab -
	(crontab -l 2>/dev/null; echo "${CRON_MINUTE} ${CRON_HOUR} 1 * * if [ ! "$(docker ps -qa -f name=optimal_k)" ]; then docker run --name optimal_k optimalk; else docker start optimal_k; fi") | crontab -
}

remove_containers ()
{
	docker rm daily_clustering weekly_clustering monthly_clustering yearly_clustering optimal_k
}


remove_containers
remove_db_hdfs
pull_images
# create_network
create_containers
add_crontab
