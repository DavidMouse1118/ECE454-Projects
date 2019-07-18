#!/bin/bash

# Aggregate throughput: 5871 RPCs/s
# 120392 [main-EventThread] INFO A3Client  - ZooKeeper event WatchedEvent state:SyncConnected type:NodeChildrenChanged path:/z498zhan
# 120393 [main-EventThread] INFO A3Client  - Found primary eceubuntu1:11888
# Average latency: 1.36 ms
# 120396 [Curator-Framework-0] INFO org.apache.curator.framework.imps.CuratorFrameworkImpl  - backgroundOperationsLoop exiting
# 120405 [main] INFO org.apache.zookeeper.ZooKeeper  - Session: 0x16bfb0f84a8fe9d closed
# 120406 [main-EventThread] INFO org.apache.zookeeper.ClientCnxn  - EventThread shut down for session: 0x16bfb0f84a8fe9d
# --- Analyzing linearizability
# Number of get operations returning junk: 0
# Number of other linearizability violations: 0


# Aggregate throughput: 5632 RPCs/s
# Average latency: 1.41 ms
# 120480 [Curator-Framework-0] INFO org.apache.curator.framework.imps.CuratorFrameworkImpl  - backgroundOperationsLoop exiting
# 120489 [main] INFO org.apache.zookeeper.ZooKeeper  - Session: 0x16bfb0f84a8ff04 closed
# 120490 [main-EventThread] INFO org.apache.zookeeper.ClientCnxn  - EventThread shut down for session: 0x16bfb0f84a8ff04
# --- Analyzing linearizability
# Number of get operations returning junk: 0
# Number of other linearizability violations: 0

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

# 140 seconds
end=$((SECONDS + 140))

# & at the end to run server with max heap 2G in parallel
$JAVA_HOME/bin/java -Xmx2g StorageNode $(hostname) $KV_PORT $ZKSTRING /$USER &
primary=$!
# ensure server on KV_PORT is primary
sleep 1s
$JAVA_HOME/bin/java -Xmx2g StorageNode $(hostname) $KV_PORT2 $ZKSTRING /$USER &
backup=$!
sleep 5s

while [ $SECONDS -lt $end ]; do
    kill -9 $primary
    # echo killed primary: $primary
    primary=$backup
    sleep 0.5s
    $JAVA_HOME/bin/java -Xmx2g StorageNode $(hostname) $KV_PORT $ZKSTRING /$USER &
    backup=$!
    # echo started backup: $backup
    sleep 0.5s
    kill -9 $primary
    # echo killed primary: $primary
    primary=$backup
    sleep 0.5s
    $JAVA_HOME/bin/java -Xmx2g StorageNode $(hostname) $KV_PORT2 $ZKSTRING /$USER &
    backup=$!
    # echo started backup: $backup
    sleep 0.5s
done

sleep 1s
kill -9 $primary
kill -9 $backup
