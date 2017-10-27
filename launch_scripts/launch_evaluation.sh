#!/bin/bash
source evaluation_suites_functions.sh

if [[ "$#" -lt 1 ]]; then
    echo "launch_evaluation <experiment_label>"
    exit 1
fi

export RESULTS_DIR=$RESULTS_DIR/$1

rm -r $RESULTS_DIR
mkdir -p $RESULTS_DIR

echo; echo; echo;
echo "Launching series..."
sleep 2
launch_series_1tg

echo; echo; echo;
echo "Launching series (separated TG)..."
sleep 2
launch_series_ntg

echo; echo; echo;
echo "Launching parallel"
sleep 2
launch_parallel_1tg

echo; echo; echo;
echo "Launching parallel (separated TG)..."
sleep 2
launch_parallel_ntg

echo; echo; echo;
echo "Launching keyspace..."
sleep 2
launch_keyspace

echo; echo; echo;
echo "Launching querying..."
sleep 2
launch_query

