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
        echo "Input: <average_query_size> <params...>"
        return 1
    fi

    local average_query_size=$1

    launch 'query_'$average_query_size $QUERY_EVAL_CLASS \
        --avg $average_query_size --inputRate 1000 "${@:2}"
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

function launch_bank_example_pure {
    launch pure_flink_tp $BANK_EXAMPLE_NOT_CLASS "$@"
    sleep 1

    # run for calculating latency
    launch pure_flink_lat $BANK_EXAMPLE_NOT_CLASS \
      --inputRate 10000 --runtimeSeconds 90 "$@"
    sleep 1
}

function launch_flink_wordcount {
    launch go_flink_go $PURE_FLINK_CLASS "${@:1}"
    sleep 1
}

function launch_recovery {
    if [[ "$#" -lt 1 ]]; then
        echo "Input: <csv_list_of_ips_in_cluster> <params...>"
        return 1
    fi

    local csv_ips=$1

    launch recovery $RECOVERY_CLASS --taskmanagers $csv_ips "${@:2}"
    sleep 1
}

# The default scenario with durability enabled
function launch_durability {
  launch durability $EVAL_CLASS --noStates 1 --noTG 1 --series true \
    --durable true "$@"
  sleep 1
}

function launch_scalability {
    if [[ "$#" -lt 1 ]]; then
        echo "Input: <scale_factor> <params...>"
        return 1
    fi

    local scale=$1

    launch 'scale_'$scale $EVAL_CLASS --noStates 1 --noTG 1 --series true \
        --par $scale --partitioning $scale "${@:2}"
}

# Launches the mixed job (voting example)
# Preferrably use LB-PL3 (high contention workload)
function launch_mixed {
    if [[ "$#" -lt 3 ]]; then
        echo "Input: <window_size_seconds> <window_slide_milliseconds> <analytics_only> <params...>"
        return 1
    fi

    local wsize=$1
    local wslide=$2
    local analytics=$3

    launch "mixed_$wsize"_"$wslide"_"$analytics" $MIXED_CLASS \
      --windowSizeSeconds $wsize \
      --windowSlideMilliseconds $wslide \
      --analyticsOnly $analytics "${@:4}"
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
    _launch_suite_keyspace 100000,70000,40000,10000,7000,4000,1000,700,400,100
}

function launch_suite_query {
    launch_query 1
    launch_query 10
    launch_query 100
    launch_query 1000
}

function launch_suite_scalability {
    launch_scalability 8 --sourcePar 4 # the real parallelism is 8 - 4
    launch_scalability 12 --sourcePar 4
    launch_scalability 20 --sourcePar 4
    launch_scalability 36 --sourcePar 4
    launch_scalability 50 --sourcePar 4
}

function launch_suite_mixed {
    if [[ "$#" -lt 1 ]]; then
        echo "Input: <base_window_slide_milliseconds> <params...>"
        return 1
    fi

    # increase size with fixed slide
    local slide=$1
    for analytics in false true; do
      launch_mixed 90 $slide $analytics "${@:2}"
      launch_mixed 120 $slide $analytics "${@:2}"
      launch_mixed 150 $slide $analytics "${@:2}"
      launch_mixed 180 $slide $analytics "${@:2}"
      launch_mixed 210 $slide $analytics "${@:2}"
    done

    # decrease slide with fixed size
    local size=90
    for analytics in false true; do
      launch_mixed $size $(($slide - 500)) $analytics "${@:2}"
      launch_mixed $size $(($slide - 1000)) $analytics "${@:2}"
      launch_mixed $size $(($slide - 1500)) $analytics "${@:2}"
      launch_mixed $size $(($slide - 2000)) $analytics "${@:2}"
    done
}
