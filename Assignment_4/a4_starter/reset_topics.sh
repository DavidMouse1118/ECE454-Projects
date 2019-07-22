#!/bin/bash

#
# Wojciech Golab, 2017
#

source ./settings.sh

echo --- Resetting topics

./delete_topic.sh $STOPIC
./create_topic.sh $STOPIC
./delete_topic.sh $CTOPIC
./create_topic.sh $CTOPIC
./delete_topic.sh $OTOPIC
./create_topic.sh $OTOPIC

./list_topics.sh

