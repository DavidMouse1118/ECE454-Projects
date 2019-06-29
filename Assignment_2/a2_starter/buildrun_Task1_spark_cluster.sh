#!/bin/sh

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export SCALA_HOME=/usr
export CLASSPATH=".:/opt/spark-latest/jars/*"

echo --- Deleting
rm Task1.jar
rm Task1*.class

echo --- Compiling
$SCALA_HOME/bin/scalac -J-Xmx1g Task1.scala
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf Task1.jar Task1*.class

echo --- Running
INPUT=/tmp/smalldata_z498zhan.txt
OUTPUT=/user/${USER}/a2_starter_code_output/

hdfs dfs -rm -R $OUTPUT
hdfs dfs -copyFromLocal -f sample_input/smalldata_z498zhan.txt /tmp
time spark-submit --master yarn --class Task1 Task1.jar $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT

rm -rf output_spark
mkdir output_spark/
hdfs dfs -copyToLocal $OUTPUT/part* output_spark/
