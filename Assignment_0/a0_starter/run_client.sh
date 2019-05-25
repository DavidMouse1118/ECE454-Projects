#!/bin/bash

echo --- Deleting
rm -f *.jar
rm -f *.class

echo --- Compiling
javac *.java
if [ $? -ne 0 ]; then
    exit
fi

echo --- Running
SAMPLE_INPUT=sample_input/large.txt
SAMPLE_OUTPUT=sample_output/large.out
# echo -n "Enter the server's host name or IP address: "
# read SERVER_HOST
# echo -n "Enter the server's TCP port number: "
# read SERVER_PORT
SERVER_OUTPUT=myoutput.txt
java -Xmx1g CCClient localhost 10000 $SAMPLE_INPUT $SERVER_OUTPUT

echo --- Comparing server\'s output against sample output
java Compare $SERVER_OUTPUT $SAMPLE_OUTPUT
