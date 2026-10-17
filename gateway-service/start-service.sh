#!/bin/bash

APP_NAME="gateway-service"
JAR_FILE="lib/gateway-service-1.0.0.jar"
CONFIG_DIR="config"
LOG_DIR="logs"

mkdir -p $LOG_DIR

JAVA_OPTS="-Xms512m -Xmx1024m"
JAVA_OPTS="$JAVA_OPTS -XX:+UseG1GC"
JAVA_OPTS="$JAVA_OPTS -XX:MaxGCPauseMillis=200"
JAVA_OPTS="$JAVA_OPTS -XX:+HeapDumpOnOutOfMemoryError"
JAVA_OPTS="$JAVA_OPTS -XX:HeapDumpPath=$LOG_DIR/heap_dump.hprof"
JAVA_OPTS="$JAVA_OPTS -Xloggc:$LOG_DIR/gc.log"
JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCDetails"
JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCDateStamps"

SPRING_OPTS="--spring.config.location=classpath:/,file:./config/"

if [ -f "$CONFIG_DIR/application.yml" ]; then
    SPRING_OPTS="$SPRING_OPTS --spring.config.additional-location=file:./config/"
fi

echo "Starting $APP_NAME..."
echo "Java Options: $JAVA_OPTS"
echo "Spring Options: $SPRING_OPTS"

nohup java $JAVA_OPTS -jar $JAR_FILE $SPRING_OPTS > $LOG_DIR/console.log 2>&1 &

echo $! > $APP_NAME.pid
echo "$APP_NAME started with PID $(cat $APP_NAME.pid)"
