#!/usr/bin/env bash

cd /usr/local/hadoop/etc/hadoop/
while true; do
hadoop jar /usr/local/hadoop/etc/hadoop/camus-example-0.1.0-SNAPSHOT-shaded.jar com.linkedin.camus.etl.kafka.CamusJob -P /usr/local/camus/camus-example/src/main/resources/camus.properties
sleep 10m
done