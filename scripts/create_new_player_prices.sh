#!/usr/bin/env bash

cd /home/ubuntu/projects/ff-fisher/data
while true; do
python create_player_prices.py

sudo -u ubuntu /usr/local/hadoop/bin/hdfs dfs -put player_prices.dat /user/data
sleep 10m
done