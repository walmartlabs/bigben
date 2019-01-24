#!/bin/sh
java -server -XX:+UnlockExperimentalVMOptions \
-XX:InitialRAMFraction=2 -XX:MinRAMFraction=2 -XX:+UseG1GC -XX:MaxGCPauseMillis=100 \
-XX:+UseStringDeduplication -jar bigben.jar