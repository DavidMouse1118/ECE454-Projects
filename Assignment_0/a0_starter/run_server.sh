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
RANDOM_PORT=`shuf -i 10000-10999 -n 1`
echo randomly chose port $RANDOM_PORT
java -Xmx1g CCServer $RANDOM_PORT
