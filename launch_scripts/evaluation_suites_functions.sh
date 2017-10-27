#!/bin/bash
source launch_job_functions.sh

function launch_suite {
    if [[ "$#" -lt 3 ]]; then
        echo "launch_suite <prop_to_scale> <scale_bound> <output_file> <params...>"
        return 1
    fi

    local prop=$1
    local to=$2
    local out=$3

    for i in $(seq 1 $to); do
        launch $EVAL_CLASS --output $out'_'$i "--$prop $i" "${@:4}"
        sleep 1
    done
}

function launch_suite_latency {
    if [[ "$#" -lt 4 ]]; then
        echo "launch_suite_latency <prop_to_scale> <scale_bound> \ "
        echo "    <rate1,rate2,...> <output_file> <params...>"
        return 1
    fi

    local prop=$1
    local to=$2
    local out=$4
    local rates=(`echo $3 | tr ',' '\n'`)

    if [[ ${#rates[@]} -ne $to ]]; then
        echo "Pass the same number of rates as scale_bound, please"
        return 1
    fi

    for i in $(seq 1 $to); do
        launch $EVAL_CLASS --output $out'_'$i "--$prop $i" --inputRate ${rates[$(($i - 1))]} "${@:5}"
        sleep 1
    done
}

function launch_suite_keyspace {
    if [[ "$#" -lt 2 ]]; then
        echo "launch_suite_keyspace <ks1,ks2,...> <output_file> <params...>"
        return 1
    fi

    local out=$2
    local kss=(`echo $1 | tr ',' '\n'`)

    for ks in ${kss[@]}; do
        launch $EVAL_CLASS --output $out'_'$ks --ks $ks "${@:3}"
        sleep 1
    done
}

function launch_suite_query {
    if [[ "$#" -lt 3 ]]; then
        echo "launch_suite_query <queryPerc> <qf1,qf2,...> <output_file> <params...>"
        return 1
    fi

    local query_perc=$1
    local query_freqs=(`echo $2 | tr ',' '\n'`)
    local out=$3

    for qf in ${query_freqs[@]}; do
        launch $EVAL_CLASS --output $out'_'$qf --queryOn true --queryPerc $query_perc --queryRate $qf "${@:4}"
        sleep 1
    done
}

### Builtin suites
function launch_series_1tg {
    launch_suite noStates 5 $RESULTS_DIR/series_1tg --noTG 1 --series true
}

function launch_series_ntg {
    launch_suite noTG 5 $RESULTS_DIR/series_ntg --noStates 1 --series true
}

function launch_parallel_1tg {
    launch_suite noStates 5 $RESULTS_DIR/parallel_1tg --noTG 1 --series false
}

function launch_parallel_ntg {
    launch_suite noTG 5 $RESULTS_DIR/parallel_ntg --noStates 1 --series false
}

function launch_keyspace {
    launch_suite_keyspace 100000,70000,40000,10000,7000,4000,1000,700,400,100,70,40,10 \
        $RESULTS_DIR/keyspace --noTG 1 --noStates 1 --series true
}

# keyspace 50, queryPerc 0.1
# - launch one with no updates and only querying to get the throughput
# - 4500 updates/s and variable querying frequency (10, 100, 1000, 3000, 5000)
#   measure PUT and GET latency
function launch_query {
    launch --queryOn true --transfersOn false --queryPerc 0.1 --ks 50 \
        --output $RESULTS_DIR/query_throughput --noTG 1 --noStates 1 --series true

    sleep 2

    launch_suite_query 0.1 10,100,1000,3000,5000 \
        $RESULTS_DIR/query --noTG 1 --noStates 1 --ks 50 --inputRate 4500 --series true
}

