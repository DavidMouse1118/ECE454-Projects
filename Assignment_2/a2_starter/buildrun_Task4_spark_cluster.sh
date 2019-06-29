#!/bin/sh

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export SCALA_HOME=/usr
export CLASSPATH=".:/opt/spark-latest/jars/*"

echo --- Deleting
rm Task4.jar
rm Task4*.class

echo --- Compiling
$SCALA_HOME/bin/scalac -J-Xmx1g Task4.scala
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf Task4.jar Task4*.class

echo --- Running
INPUT=/tmp/in2.txt
OUTPUT=/user/${USER}/a2_starter_code_output/

hdfs dfs -rm -R $OUTPUT
hdfs dfs -copyFromLocal sample_input/smalldata_z498zhan.txt /tmp
time spark-submit --master yarn --class Task4 Task4.jar $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT

rm -rf output_spark
mkdir output_spark/
hdfs dfs -copyToLocal $OUTPUT/part* output_spark/
