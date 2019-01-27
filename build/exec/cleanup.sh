#!/usr/bin/env bash
set -e
ps aux | grep bigben.jar | grep -v grep | awk '{print $2}' | xargs kill -9