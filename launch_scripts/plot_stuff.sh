#!/bin/bash
if [[ "$#" -lt 1 ]]; then
    echo "plot_stuff <folder_with_results>"
    exit 1
fi

folder=$1
python plot_bank_isolation.py $folder
python plot_overview.py $folder
python plot_ks_comparison.py $folder
python plot_querying.py $folder
python plot_scalability.py $folder
