#!/bin/bash

base_dir=$(dirname $0)

if [ "x$KAFKA_HEAP_OPTS" = "x" ]; then
    export KAFKA_HEAP_OPTS="-Xmx2G -Xms1G"
fi

EXTRA_ARGS=${EXTRA_ARGS-'-name streamsServer -loggc java -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=8008
-Dcom.sun.management.jmxremote.authenticate=false'}

exec java -jar $KAFKA_HEAP_OPTS $EXTRA_ARGS streams-pipeline-1.0.jar "$@"