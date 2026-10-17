@echo off
setlocal enabledelayedexpansion

set APP_NAME=gateway-service
set JAR_FILE=lib\gateway-service-1.0.0.jar
set CONFIG_DIR=config
set LOG_DIR=logs

if not exist %LOG_DIR% mkdir %LOG_DIR%

set JAVA_OPTS=-Xms512m -Xmx1024m
set JAVA_OPTS=%JAVA_OPTS% -XX:+UseG1GC
set JAVA_OPTS=%JAVA_OPTS% -XX:MaxGCPauseMillis=200
set JAVA_OPTS=%JAVA_OPTS% -XX:+HeapDumpOnOutOfMemoryError
set JAVA_OPTS=%JAVA_OPTS% -XX:HeapDumpPath=%LOG_DIR%\heap_dump.hprof

set SPRING_OPTS=--spring.config.location=classpath:/,file:.\config\

echo Starting %APP_NAME%...
echo Java Options: %JAVA_OPTS%
echo Spring Options: %SPRING_OPTS%

start /B java %JAVA_OPTS% -jar %JAR_FILE% %SPRING_OPTS% > %LOG_DIR%\console.log 2>&1

echo %APP_NAME% started
endlocal
