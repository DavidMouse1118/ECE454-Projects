#!/bin/bash

#
# Wojciech Golab, 2017
#

source ./settings.sh

${KAFKA_HOME}/bin/kafka-console-producer.sh --broker-list $KBROKERS --topic $STOPIC \
    --property parse.key=true --property key.separator=,
