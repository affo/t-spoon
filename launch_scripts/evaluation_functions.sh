#!/bin/bash
source launch_job_functions.sh

function _launch_suite {
    if [[ "$#" -lt 3 ]]; then
        echo "launch_suite <label> <prop_to_scale> <scale_bound> <params...>"
        return 1
    fi

    local label=$1
    local prop=$2
    local to=$3

    for i in $(seq 1 $to); do
        launch $label'_'$i $EVAL_CLASS "--$prop $i" "${@:4}"
        sleep 1
    done
}

function _launch_suite_keyspace {
    if [[ "$#" -lt 1 ]]; then
        echo "launch_suite_keyspace <ks1,ks2,...> <params...>"
        return 1
    fi

    local kss=(`echo $1 | tr ',' '\n'`)

    for ks in ${kss[@]}; do
        launch_keyspace $ks "${@:2}"
        sleep 1
    done
}


### Builtin evaluation functions
function _launch_tgs {
    if [[ "$#" -lt 3 ]]; then
        echo "Input: <number_of_states> <number_of_tgs> <series_or_parallel> <params...>"
        return 1
    fi

    local no_states=$1
    local no_tgs=$2
    local series=$3

    local label=""

    if [[ $series = true ]]; then
        label="series_"
    else
        label="parallel_"
    fi

    if [[ $no_tgs -gt 1 ]]; then
        label="$label""ntg_$no_tgs"
    else
        label="$label""1tg_$no_states"
    fi

    launch $label $EVAL_CLASS --noStates $no_states --noTG $no_tgs --series $series "${@:4}"
    sleep 1
}

function launch_series_1tg {
    if [[ "$#" -lt 1 ]]; then
        echo "Input: <number_of_states> <params...>"
        return 1
    fi

    _launch_tgs $1 1 true "${@:2}"
}

function launch_series_ntg {
    if [[ "$#" -lt 1 ]]; then
        echo "Input: <number_of_tgs> <params...>"
        return 1
    fi

    _launch_tgs 1 $1 true "${@:2}"
}

function launch_parallel_1tg {
    if [[ "$#" -lt 1 ]]; then
        echo "Input: <number_of_states> <params...>"
        return 1
    fi

    _launch_tgs $1 1 false "${@:2}"
}

function launch_parallel_ntg {
    if [[ "$#" -lt 1 ]]; then
        echo "Input: <number_of_tgs> <params...>"
        return 1
    fi

    _launch_tgs 1 $1 false "${@:2}"
}

function launch_keyspace {
    if [[ "$#" -lt 1 ]]; then
        echo "Input: <keyspace_size> <params...>"
        return 1
    fi

    local ks=$1

    launch 'keyspace_'$ks $EVAL_CLASS --ks $ks \
      --noStates 1 --noTG 1 --series true "${@:2}"
}

function launch_query {
    if [[ "$#" -lt 1 ]]; then
        echo "Input: <update_frequency> <params...>"
        return 1
    fi

    local update_frequency=$1

    launch 'query_'$update_frequency $QUERY_EVAL_CLASS \
        --inputRate $update_frequency "${@:2}"
}

function launch_consistency_check {
    launch consistency_check $CONSISTENCY_CHECK_CLASS "${@:1}"
    sleep 1
}

function launch_bank_example {
    if [[ "$#" -lt 1 ]]; then
        echo "Input: <transactional_guarantees?(0/1)> <params...>"
        return 1
    fi

    local guarantees=$1

    local class=$BANK_EXAMPLE_NOT_CLASS

    if [[ $guarantees -eq 0 ]]; then
      class=$BANK_EXAMPLE_CLASS
    fi

    launch bank_example $class "${@:2}"
    sleep 1
}

function launch_flink_wordcount {
    launch go_flink_go $PURE_FLINK_CLASS "${@:1}"
    sleep 1
}

### Builtin suites
function launch_suite_series_1tg {
    _launch_suite series_1tg noStates 5 --noTG 1 --series true
}

function launch_suite_series_ntg {
    _launch_suite series_ntg noTG 5 --noStates 1 --series true
}

function launch_suite_parallel_1tg {
    _launch_suite parallel_1tg noStates 5 --noTG 1 --series false
}

function launch_suite_parallel_ntg {
    _launch_suite parallel_ntg noTG 5 --noStates 1 --series false
}

function launch_suite_keyspace {
    _launch_suite_keyspace 100000,70000,40000,10000,7000,4000,1000,700,400,100,70,40,10
}

# keyspace 50, queryPerc 0.1
# - launch one with no updates and only querying to get the throughput
# - 2000 updates/s and variable querying frequency (10, 100, 1000, 3000, 5000)
#   measure PUT and GET latency
function launch_suite_query {
    #launch --queryOn true --transfersOn false --queryPerc 0.1 --ks 50 \
    #    --output $RESULTS_DIR/query_throughput --noTG 1 --noStates 1 --series true

    #sleep 2

    launch_query 1000
    launch_query 3000
    launch_query 5000
}
