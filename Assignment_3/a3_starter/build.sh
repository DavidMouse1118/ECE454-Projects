#!/bin/bash

#
# Wojciech Golab, 2016
#

source settings.sh

JAVA_CC=$JAVA_HOME/bin/javac
THRIFT_CC=./thrift-0.12.0

export CLASSPATH=".:gen-java/:lib/*"

echo --- Cleaning
rm -f *.class
rm -fr gen-java

echo --- Compiling Thrift IDL
$THRIFT_CC --version &> /dev/null
ret=$?
if [ $ret -ne 0 ]; then
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    echo "ERROR: The Thrift compiler does not work on this host."
    echo "       Please build on one of the eceubuntu or ecetesla hosts."
    echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    exit 1
fi
$THRIFT_CC --version
$THRIFT_CC --gen java:generated_annotations=suppress a3.thrift

echo --- Compiling Java
$JAVA_CC -version
$JAVA_CC gen-java/*.java
$JAVA_CC *.java

