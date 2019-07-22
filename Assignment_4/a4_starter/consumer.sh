#!/bin/bash

#
# Wojciech Golab, 2017
#

source ./settings.sh

${KAFKA_HOME}/bin/kafka-console-consumer.sh \
    --bootstrap-server $KBROKERS \
    --topic $OTOPIC \
    --property print.key=true \
    --property print.value=true \
    --property key.separator=, \
    --from-beginning \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

#    --formatter kafka.tools.DefaultMessageFormatter \
