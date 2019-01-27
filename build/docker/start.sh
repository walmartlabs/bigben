#!/bin/sh
DEFAULT_JAVA_OPTS="-server -XX:+UnlockExperimentalVMOptions -XX:InitialRAMFraction=2 -XX:MinRAMFraction=2 -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+UseStringDeduplication"
if [[ "x${JAVA_OPTS}" != "x" ]]; then export JAVA_OPTS="${DEFAULT_JAVA_OPTS} ${JAVA_OPTS}"; else export JAVA_OPTS="${DEFAULT_JAVA_OPTS}"; fi
echo "using JAVA_OPTS: ${JAVA_OPTS}"
java ${JAVA_OPTS} -jar bigben.jar