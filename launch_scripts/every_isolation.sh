#!/bin/bash
source evaluation_functions.sh

if [[ "$#" -lt 2 ]]; then
    echo "launch_evaluation <experiment_label> <launch_fn>"
    exit 1
fi

exp_label="$1"
fn="$2"

function launch_every {
  BASE_RESULTS_DIR=$1
  fn=$2

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
          echo "Launching $fn..."
          eval "$fn ${@:3}"
      done
  done
}

export RESULTS_DIR=$RESULTS_DIR/$exp_label
BASE_RESULTS_DIR=$RESULTS_DIR

rm -r $RESULTS_DIR
mkdir -p $RESULTS_DIR

# launch experiments
launch_every $BASE_RESULTS_DIR $fn "${@:3}"
