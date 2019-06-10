#!/bin/sh

# export JAVA_TOOL_OPTIONS='-Xmx4g -Xss4m'

echo "Spawning 20 Client nodes"
p="password"
for i in `seq 1 20` ;
do
    b="$i" 
    password="passssssssssword"

    echo "Client Password: $password"
    java -cp .:gen-java/:"lib/*":"jBCrypt-0.4/*" Client localhost 10230 hello
done