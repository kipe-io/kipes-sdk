#!/bin/bash

#### GLOBAL VARS ##############################################################

BOOTSTRAP_SERVER='pkc-lq8v7.eu-central-1.aws.confluent.cloud:9092'
CACHE_FOLDER=/tmp/kafka-streams
CONFIG_FILE=~/.kafka/client.config.confluent

#### FUNCTIONS ################################################################

####
#
# parameters:
# $1 - search regexp
function find_topics() {
	kafka-topics.sh 2> /dev/null \
	--bootstrap-server "${BOOTSTRAP_SERVER}" \
	--command-config ${CONFIG_FILE} \
	--list \
	| egrep "${1}"
}

####
#
# parameters:
# $1 - the topic to be deleted
function delete_topic() {

	echo "delete topic: '${1}'"
	
	kafka-topics.sh 2> /dev/null \
	--bootstrap-server "${BOOTSTRAP_SERVER}" \
	--command-config ${CONFIG_FILE} \
	--topic "${1}" \
	--delete
	
}

function remove_cache() {

	echo "cleaning cache"
	rm -rf ${CACHE_FOLDER}
}

####
#
# parameters:
# $1 - search regexp
function find_consumer_groups() {

	kafka-consumer-groups.sh 2> /dev/null \
	--bootstrap-server "${BOOTSTRAP_SERVER}" \
	--command-config ${CONFIG_FILE} \
	--list \
	| egrep "${1}"	
}

####
#
# parameters:
# $1 - consumer group name
# $2 - topic
function reset_consumer_group_offsets() {
	
	kafka-consumer-groups.sh 2> /dev/null \
	--bootstrap-server "${BOOTSTRAP_SERVER}" \
	--command-config ${CONFIG_FILE} \
	--group "${1}" \
	--reset-offsets \
	--all-topics \
	--to-earliest \
	--execute
	
}

#### SCRIPT ###################################################################

if [ -z "${1}" ]; then
	echo "Missing topic regexp to identify topics to be deleted. Aborting"
	exit 1
fi

REGEXP="${1}"

# delete topics ---------------------------------------------------------------

TOPICS=$( find_topics "${REGEXP}" )

if [ ! -z "${TOPICS}" ]; then
	echo
	echo "Found following topics:"
	echo -e "${TOPICS}"
	echo
	echo -n "Are you sure to delete those topics? [y/N]: "
	
	read ANSWER
	
	if [ "y" != "${ANSWER}" ]; then
		exit
	fi
	
	echo
	
	echo -e "${TOPICS}" \
	| while read TOPIC; do
		delete_topic "${TOPIC}"
	done
	
	echo
	
	# clean cache	
	remove_cache
	
else 
	echo
	echo "No topics matching '${REGEXP}' found."
fi

# reset consumer group offsets ------------------------------------------------

CONSUMER_GROUPS=$( find_consumer_groups "${REGEXP}" )

if [ ! -z "${CONSUMER_GROUPS}" ]; then

	echo
	echo "Found following consumer groups:"
	echo -e "${CONSUMER_GROUPS}"
	echo 
	echo -n "Do you want to reset related offsets for these group(s)? [Y/n]: "
	
	read ANSWER
	
	if [ ! -z "${ANSWER}" -a "y" != "${ANSWER}" -a "Y" != "${ANSWER}" ]; then
		exit
	fi
	
	echo
	
	echo -e "${CONSUMER_GROUPS}" \
	| while read GROUP; do
		reset_consumer_group_offsets "${GROUP}"
	done
	
fi
