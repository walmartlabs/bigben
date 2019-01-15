#!/bin/sh
PROPS_URI="uri://"${APP_ROOT}"/bigben-config.yaml"
echo "Using bigben config file: "${PROPS_URI}
java -server -Dbigben.props=${PROPS_URI} -XX:+UnlockExperimentalVMOptions -XX:InitialRAMFraction=2 -XX:MinRAMFraction=2 -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+UseStringDeduplication -jar bigben.jar