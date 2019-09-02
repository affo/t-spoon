#!/bin/bash

if [[ "$#" -lt 1 ]]; then
    echo "$0 <singlePartitionTxPerc> [<results_folder>]"
    echo "    e.g. $0 25"
    exit 1
fi

now=$(date +"%s")
perc=$1
resf=${2:-"$0_results"}
mkdir -p "$resf"
tp_fname="$resf/$now""__tp_$perc"
lat_fname="$resf/$now""__lat_$perc"
global_fname="$resf/global.csv"


echo "Installing..."
./voltdb_install.sh

echo "Running throughput experiment...."
echo "Saving output to $tp_fname"
./run_benchmark.sh --singlePartitionTxPerc "$perc" | tee "$tp_fname"
tp=$(cat $tp_fname | grep "Average throughput" | awk '{print $3}')
# remove dot
tp="${tp//.}"

# half the tp is the threshold for latency
tpt=$(($tp / 2))
echo "Running latency experiment...."
echo "Saving output to $lat_fname"
./run_benchmark.sh --singlePartitionTxPerc "$perc" --ratelimit "$tpt" --tuples 20000 --preload false | tee "$lat_fname"
# TODO should this be the internal latency or not?
# lat=$(cat $lat_fname | grep "Average latency" | awk '{print $3}')
lat=$(cat $lat_fname | grep "Reported Internal Avg Latency" | awk '{print $5}')
# replace comma with dot
lat="${lat//,/.}"

echo "$now,$perc,$tp,$lat" >> "$global_fname"
