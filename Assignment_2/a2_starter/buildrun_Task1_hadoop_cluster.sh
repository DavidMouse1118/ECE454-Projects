#!/bin/sh

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export CLASSPATH=`hadoop classpath`

echo --- Deleting
rm Task1.jar
rm Task1*.class

echo --- Compiling
$JAVA_HOME/bin/javac Task1.java
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
time yarn jar Task1.jar Task1 $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT

rm -rf output_hadoop
mkdir output_hadoop/
hdfs dfs -copyToLocal $OUTPUT/part* output_hadoop/
