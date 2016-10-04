#!/usr/bin/env bash

while true; do
spark-submit --packages datastax:spark-cassandra-connector:1.6.0-s_2.10 --conf spark.cassandra.connection.host='172.31.0.107' /home/ubuntu/projects/ff-fisher/batch_processing/calc_lifetime_totals.py
sleep 60m
done