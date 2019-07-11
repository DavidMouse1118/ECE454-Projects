#!/bin/bash

source settings.sh

JAVA_CC=$JAVA_HOME/bin/javac
export CLASSPATH=".:gen-java:lib/*"


echo --- Running client
# args: 4 threads, 5 seconds, keys drawn from a set of 1000
$JAVA_HOME/bin/java A3Client $ZKSTRING /$USER 4 10 1000

echo --- Analyzing linearizability
$JAVA_HOME/bin/java ca.uwaterloo.watca.LinearizabilityTest execution.log scores.txt > /dev/null
echo Number of get operations returning junk: `cat scores.txt | grep 'Score = 2' | wc -l`
echo Number of other linearizability violations: `cat scores.txt | grep 'Score = 1' | wc -l`

echo
echo NOTE: Shut down both the primary and the secondary storage nodes after running this script, and then restart them.  Failure to do so will result in false linearizability violations the next time you run the client.
echo 
