#!/usr/bin/env bash

CLUSTER_NAME=$1

peg install ${CLUSTER_NAME} zookeeper
wait
peg install ${CLUSTER_NAME} kafka
wait
peg install ${CLUSTER_NAME} hadoop
wait

peg service ${CLUSTER_NAME} zookeeper start
wait
peg service ${CLUSTER_NAME} kafka start
wait
echo "all things installed and started"