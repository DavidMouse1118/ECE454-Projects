#!/bin/bash

source settings.sh

JAVA_CC=$JAVA_HOME/bin/javac
export CLASSPATH=".:gen-java:lib/*"

# change your port number
KV_PORT=11118
KV_PORT_2=11888
echo Port number: $KV_PORT
echo Port number 2: $KV_PORT_2

kill -9 $(lsof -i:$KV_PORT -t) $(lsof -i:$KV_PORT_2 -t)

end=$((SECONDS + 130))

$JAVA_HOME/bin/java -Xmx2g StorageNode $(hostname) $KV_PORT $ZKSTRING /$USER &
P1=$!
sleep 1s
$JAVA_HOME/bin/java -Xmx2g StorageNode $(hostname) $KV_PORT_2 $ZKSTRING /$USER &
P2=$!
sleep 7s

while [ $SECONDS -lt $end ]; do
    KILL_P1=$(shuf -i 0-1 -n 1)

    if [ "$KILL_P1" -eq 1 ]; then
        kill -9 $P1
        sleep 1.5s
        $JAVA_HOME/bin/java -Xmx2g StorageNode $(hostname) $KV_PORT $ZKSTRING /$USER &
        P1=$!
        sleep 1.5s
    else
        kill -9 $P2
        sleep 1.5s
        $JAVA_HOME/bin/java -Xmx2g StorageNode $(hostname) $KV_PORT_2 $ZKSTRING /$USER &
        P2=$!
        sleep 1.5s
    fi
done

kill -9 $P1 $P2