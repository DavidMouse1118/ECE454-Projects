#!/bin/sh

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export CLASSPATH=`hadoop classpath`

echo --- Deleting
rm HadoopWC.jar
rm HadoopWC*.class

echo --- Compiling
$JAVA_HOME/bin/javac HadoopWC.java
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf HadoopWC.jar HadoopWC*.class

echo --- Running
INPUT=/tmp/smalldata_z498zhan.txt
OUTPUT=/user/${USER}/a2_starter_code_output/

hdfs dfs -rm -R $OUTPUT
hdfs dfs -copyFromLocal sample_input/smalldata_z498zhan.txt /tmp
time yarn jar HadoopWC.jar HadoopWC $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT

rm -rf output_hadoop
mkdir output_hadoop/
hdfs dfs -copyToLocal $OUTPUT/part* output_hadoop/