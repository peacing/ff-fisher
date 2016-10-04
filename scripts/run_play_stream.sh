#!/usr/bin/env bash

#spark-submit --master spark://ip-172-31-0-106:7077 --driver-memory 1G --executor-memory 1G --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.2 --conf spark.cassandra.connection.host='172.31.0.104' /home/ubuntu/projects/ff-fisher/streaming/plays_stream.py

spark-submit --master spark://ip-172-31-0-106:7077 --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.2,datastax:spark-cassandra-connector:1.6.0-s_2.10 --conf spark.cassandra.connection.host='172.31.0.104' /home/ubuntu/projects/ff-fisher/streaming/plays_stream.py