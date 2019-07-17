#!/bin/bash

source settings.sh

JAVA_CC=$JAVA_HOME/bin/javac
export CLASSPATH=".:gen-java:lib/*"

# fixed port number
KV_PORT=11118
KV_PORT2=11888
echo Port number: $KV_PORT
echo Port number 2: $KV_PORT2

kill -9 $(lsof -i:$KV_PORT -t)
kill -9 $(lsof -i:$KV_PORT2 -t)

# 130 seconds
end=$((SECONDS + 130))

# & at the end to run server with max heap 2G in parallel
$JAVA_HOME/bin/java -Xmx2g StorageNode $(hostname) $KV_PORT $ZKSTRING /$USER &
# ensure server on KV_PORT is primary
sleep 1s
$JAVA_HOME/bin/java -Xmx2g StorageNode $(hostname) $KV_PORT2 $ZKSTRING /$USER &
sleep 5s

while [ $SECONDS -lt $end ]; do
    kill -9 $(lsof -i:$KV_PORT -t)
    sleep 1.5s
    $JAVA_HOME/bin/java -Xmx2g StorageNode $(hostname) $KV_PORT $ZKSTRING /$USER &
    sleep 1.5s
    kill -9 $(lsof -i:$KV_PORT2 -t)
    sleep 1.5s
    $JAVA_HOME/bin/java -Xmx2g StorageNode $(hostname) $KV_PORT2 $ZKSTRING /$USER &
    sleep 1.5s
done

sleep 1s
kill -9 $(lsof -i:$KV_PORT -t)
kill -9 $(lsof -i:$KV_PORT2 -t)
