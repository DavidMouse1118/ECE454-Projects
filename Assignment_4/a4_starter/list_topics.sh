#!/bin/bash

#
# Wojciech Golab, 2017
#

source ./settings.sh

${KAFKA_HOME}/bin/kafka-topics.sh --list --zookeeper $ZKSTRING | grep ${USER}
