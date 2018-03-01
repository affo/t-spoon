#!/bin/bash
if [[ "$#" -lt 3 ]]; then
    echo "./voltdb_deploy.sh <hostcount> <leader> <sites(/cores)>"
    exit 1
fi

source voltrc

hostcount=$1
volt_host=$2
no_sites=$3

deploy_template=./deploy_conf.template.xml
deployment_file="$DATA_DIR/deploy_conf.xml"

sed 's/##SITES##/'$no_sites'/g' $deploy_template > $deployment_file

VOLT_BIN="$VOLT_HOME/bin/voltdb"

$VOLT_BIN init --force --dir $DATA_DIR --config $deployment_file
$VOLT_BIN start --dir $DATA_DIR --ignore=thp \
            --count $hostcount --host $volt_host
