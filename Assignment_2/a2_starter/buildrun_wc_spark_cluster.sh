#!/bin/sh

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export SCALA_HOME=/usr
export CLASSPATH=".:/opt/spark-latest/jars/*"

echo --- Deleting
rm SparkWC.jar
rm SparkWC*.class

echo --- Compiling
$SCALA_HOME/bin/scalac -J-Xmx1g SparkWC.scala
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf SparkWC.jar SparkWC*.class

echo --- Running
INPUT=/tmp/smalldata.txt
OUTPUT=/user/${USER}/a2_starter_code_output/

hdfs dfs -rm -R $OUTPUT
hdfs dfs -copyFromLocal sample_input/smalldata.txt /tmp
time spark-submit --master yarn --class SparkWC SparkWC.jar $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT
