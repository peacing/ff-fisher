#!/usr/bin/env bash

CLUSTER_NAME=$1

peg service ${CLUSTER_NAME} hadoop start
wait
echo "all things installed and started"