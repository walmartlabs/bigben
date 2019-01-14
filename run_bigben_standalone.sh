#!/usr/bin/env bash
echo Building BigBen
mvn clean install
cd app/target
echo Starting BigBen
java -jar bigben.jar