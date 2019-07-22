#!/bin/bash

#
# Wojciech Golab, 2017
#

source ./settings.sh

echo --- Compiling Java
$JAVA_CC Reset*.java
if [ $? -eq 0 ]
then
  echo "Success..."
else
  echo "Error..."
  exit 1
fi

echo --- Local reset
$JAVA ResetApplication $KBROKERS $APP_NAME student-topic-$USER classroom-topic-$USER output-topic-$USER $STATE_STORE_DIR

echo --- Global reset
$KAFKA_HOME/bin/kafka-streams-application-reset.sh --application-id $APP_NAME --bootstrap-servers $KBROKERS --zookeeper $ZKSTRING --input-topics $STOPIC,$CTOPIC
