#!/bin/bash
source evaluation_functions.sh

if [[ "$#" -lt 1 ]]; then
    echo "launch_evaluation <experiment_label>"
    exit 1
fi

export RESULTS_DIR=$RESULTS_DIR/$1
BASE_RESULTS_DIR=$RESULTS_DIR

rm -r $RESULTS_DIR
mkdir -p $RESULTS_DIR

opt_isolation=(0 1 2 3)
pess_isolation=(3 4)

for opt in true false; do

    export IS_OPTIMISTIC=$opt

    isolation=()
    strategy_name=""
    if [[ $opt = true ]]; then
        isolation=${opt_isolation[*]}
        strategy_name=OPT
    else
        isolation=${pess_isolation[*]}
        strategy_name=PESS
    fi

    for level in $isolation; do

        export ISOLATION=$level
        export RESULTS_DIR=$BASE_RESULTS_DIR/$strategy_name/PL$level
        mkdir -p $RESULTS_DIR

        echo; echo ">>> Strategy: $strategy_name, Isolation level: $ISOLATION"
        echo ">>> Saving results to $RESULTS_DIR"

        echo; echo; echo;
        echo "Launching series..."
        sleep 2
        launch_suite_series_1tg

        echo; echo; echo;
        echo "Launching series (separated TG)..."
        sleep 2
        launch_suite_series_ntg

        echo; echo; echo;
        echo "Launching parallel"
        sleep 2
        launch_suite_parallel_1tg

        echo; echo; echo;
        echo "Launching parallel (separated TG)..."
        sleep 2
        launch_suite_parallel_ntg

        echo; echo; echo;
        echo "Launching keyspace..."
        sleep 2
        launch_suite_keyspace

        echo; echo; echo;
        echo "Launching querying..."
        sleep 2
        launch_suite_query
    done
done
