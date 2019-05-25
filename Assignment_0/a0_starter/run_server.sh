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
# RANDOM_PORT=`shuf -i 10000-10999 -n 1`
echo randomly chose port 10000
java -Xmx1g CCServer 10000
