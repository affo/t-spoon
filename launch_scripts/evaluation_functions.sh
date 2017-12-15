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
        launch 'keyspace_'$ks $EVAL_CLASS --ks $ks "${@:2}"
        sleep 1
    done
}

function _launch_suite_query {
    if [[ "$#" -lt 1 ]]; then
        echo "launch_suite_query <qf1,qf2,...> <params...>"
        return 1
    fi

    local query_freqs=(`echo $1 | tr ',' '\n'`)

    for qf in ${query_freqs[@]}; do
        launch_query $qf 4500 "${@:2}"
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

function launch_query {
    if [[ "$#" -lt 2 ]]; then
        echo "Input: <query_frequency> <update_frequency> <params...>"
        return 1
    fi

    local query_frequency=$1
    local update_frequency=$2
    local n_records=$(($update_frequency * 120)) # make it last 2 minutes

    launch 'query_'$query_frequency $EVAL_CLASS --queryOn true --queryPerc 0.1 \
        --noTG 1 --noStates 1 --series true \
        --ks 50 --inputRate $update_frequency --nRec $n_records --sled 0 \
        --queryRate $query_frequency "${@:3}"
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
    _launch_suite_keyspace 100000,70000,40000,10000,7000,4000,1000,700,400,100,70,40,10 \
        --noTG 1 --noStates 1 --series true
}

# keyspace 50, queryPerc 0.1
# - launch one with no updates and only querying to get the throughput
# - 2000 updates/s and variable querying frequency (10, 100, 1000, 3000, 5000)
#   measure PUT and GET latency
function launch_suite_query {
    #launch --queryOn true --transfersOn false --queryPerc 0.1 --ks 50 \
    #    --output $RESULTS_DIR/query_throughput --noTG 1 --noStates 1 --series true

    #sleep 2

    _launch_suite_query 10,100,1000,3000,5000
}

