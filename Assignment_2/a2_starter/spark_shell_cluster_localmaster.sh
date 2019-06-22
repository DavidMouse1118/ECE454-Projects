#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export SCALA_HOME=/usr/share/scala
export CLASSPATH=".:/opt/spark-latest/jars/*"

$SPARK_HOME/bin/spark-shell --master "local[2]"
