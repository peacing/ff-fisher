#!/usr/bin/env bash

CLUSTER_NAME=$1

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$DIR")"

peg scp from-local $CLUSTER_NAME 1 $PROJECT_ROOT /home/ubuntu/projects/ff-fisher