unset JAVA_TOOL_OPTIONS
JAVA=java
JAVA_CC=javac

KAFKA_HOME="./kafka_2.11-2.3.0"
export CLASSPATH=.:"${KAFKA_HOME}/libs/*"

STATE_STORE_DIR=/tmp/A4-Kafka-state-store-${USER}

ZKSTRING=manta.uwaterloo.ca:2181
KBROKERS=manta.uwaterloo.ca:9092
STOPIC=student-${USER}
CTOPIC=classroom-${USER}
OTOPIC=output-${USER}
APP_NAME=A4Application-${USER}
