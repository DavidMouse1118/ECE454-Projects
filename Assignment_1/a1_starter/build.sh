#!/bin/bash

#
# Wojciech Golab, 2016-2019
#

if [ -f /usr/lib/jvm/default-java/bin/javac ]; then
    JAVA_HOME=/usr/lib/jvm/default-java
elif [ -f /usr/lib/jvm/java-11-openjdk-amd64/bin/javac ]; then
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
elif [ -f /usr/lib/jvm/java-8-openjdk-amd64/bin/javac ]; then
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
elif [ -f /usr/lib/jvm/java-openjdk/bin/javac ]; then
    JAVA_HOME=/usr/lib/jvm/java-openjdk
else
    echo "Unable to find java compiler :("
    exit 1
fi

JAVA=$JAVA_HOME/bin/java
JAVA_CC=$JAVA_HOME/bin/javac
THRIFT_CC=./thrift-0.12.0

echo --- Cleaning
rm -f *.jar
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
$THRIFT_CC --gen java:generated_annotations=suppress a1.thrift


echo --- Compiling Java
$JAVA_CC -version
$JAVA_CC gen-java/*.java -cp .:"lib/*"
$JAVA_CC *.java -cp .:gen-java/:"lib/*":"jBCrypt-0.4/*"

echo --- Done, now run your code.
echo     Examples:
echo $JAVA '-cp .:gen-java/:"lib/*":"jBCrypt-0.4/*" FENode 10123'
echo $JAVA '-cp .:gen-java/:"lib/*":"jBCrypt-0.4/*" BENode localhost 10123 10124'
echo $JAVA '-cp .:gen-java/:"lib/*":"jBCrypt-0.4/*" Client localhost 10123 hello'

