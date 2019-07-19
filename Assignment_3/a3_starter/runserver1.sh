#!/bin/bash

source settings.sh

JAVA_CC=$JAVA_HOME/bin/javac
export CLASSPATH=".:gen-java:lib/*"

KV_PORT=10118
echo Port number: $KV_PORT

taskset -c 0,1,2,3 $JAVA_HOME/bin/java StorageNode `hostname` $KV_PORT $ZKSTRING /$USER
