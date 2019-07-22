#!/bin/bash

#
# Wojciech Golab, 2017
#

source ./settings.sh

echo --- Cleaning
rm -f A4*.class

echo --- Compiling Java
$JAVA_CC A4*.java
if [ $? -eq 0 ]
then
  echo "Success..."
else
  echo "Error..."
  exit 1
fi

echo --- Running

$JAVA A4Application $KBROKERS $APP_NAME $STOPIC $CTOPIC $OTOPIC $STATE_STORE_DIR
