#!/usr/bin/env bash

set -eu

. ~/address.sh
ssh -i ~/.ssh/paul-singman.pem ubuntu@$master_pubdns <<- 'ENDSSH'
cd /usr/local/hadoop/etc/hadoop/
hadoop jar /usr/local/hadoop/etc/hadoop/camus-example-0.1.0-SNAPSHOT-shaded.jar com.linkedin.camus.etl.kafka.CamusJob -P /usr/local/camus/camus-example/src/main/resources/camus.properties
ENDSSH