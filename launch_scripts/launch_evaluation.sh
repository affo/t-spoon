#!/bin/bash
source evaluation_functions.sh

if [[ "$#" -lt 1 ]]; then
    echo "launch_evaluation <experiment_label>"
    exit 1
fi

exp_label="$1"

function launch_topologies {
  BASE_RESULTS_DIR=$1

  opt_isolation=(2 3 4)
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
      done
  done
}

export RESULTS_DIR=$RESULTS_DIR/$exp_label
BASE_RESULTS_DIR=$RESULTS_DIR

rm -r $RESULTS_DIR
mkdir -p $RESULTS_DIR

# launch experiments
launch_topologies $BASE_RESULTS_DIR

# some experiments run only for 1 isolation level
# we'll see later what to do with latencies...

export IS_OPTIMISTIC=false
export ISOLATION=3
export RESULTS_DIR=$BASE_RESULTS_DIR/querying
mkdir -p $RESULTS_DIR
echo; echo; echo;
echo "Launching querying..."
sleep 2
launch_suite_query

export IS_OPTIMISTIC=false
export ISOLATION=3
export RESULTS_DIR=$BASE_RESULTS_DIR/durability
mkdir -p $RESULTS_DIR
echo; echo; echo;
echo "Launching durability..."
sleep 2
launch_durability

export IS_OPTIMISTIC=false
export ISOLATION=3
export RESULTS_DIR=$BASE_RESULTS_DIR/scalability
mkdir -p $RESULTS_DIR
echo; echo; echo;
echo "Launching scalability..."
sleep 2
launch_suite_scalability

export IS_OPTIMISTIC=false # more stability on high contention
export ISOLATION=3
export RESULTS_DIR=$BASE_RESULTS_DIR/mixed
mkdir -p $RESULTS_DIR
echo; echo; echo;
echo "Launching mixed experiments..."
sleep 2
launch_suite_mixed

export RESULTS_DIR=$BASE_RESULTS_DIR/pure_flink
mkdir -p $RESULTS_DIR
echo; echo; echo;
echo "Launching pure Flink..."
sleep 2
launch_bank_example_pure
