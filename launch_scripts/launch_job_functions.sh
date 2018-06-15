if [[ -e "launchrc" ]]; then
    source launchrc
    IFS=","
    read -ra TMS_ARRAY <<< "$TASK_MANAGERS"
    IFS=" "
    export TMS_ARRAY
    echo ">>> launchrc configuration loaded"
fi

if [[ -z "$FLINK_HOME" ]]; then
    echo "Set FLINK_HOME please"
    return 1
fi

if [[ -z "$TARGET_JAR" ]]; then
    echo "Set TARGET_JAR please"
    return 1
fi

if [[ -z "$PACKAGE_BASE" ]]; then
    echo "Set PACKAGE_BASE please"
    return 1
fi

FLINK_BIN=$FLINK_HOME/bin/flink

function refresh_cluster {
    if [[ $REFRESH_CLUSTER = true ]]; then
        restart-cluster
    fi
}

function sleep_if {
    if [[ $DEBUG != true ]]; then
        sleep $1
    fi
}

function generate_post_data {
    cat <<EOF
{
    "type": "note",
    "title": "$1",
    "body": "$2"
}
EOF
}

function notify {
    if [[ -z $PUSH_BULLET_ACCESS_TOKEN ]]; then
        echo ">>> No notification sent..."
    else
        echo ">>> Sending notification..."
        title="$1"
        body="$2"
        curl -s \
            --header "Access-Token: $PUSH_BULLET_ACCESS_TOKEN" \
            --header 'Content-Type: application/json' \
            --request POST https://api.pushbullet.com/v2/pushes \
            --data-binary "$(generate_post_data "$title" "$body")" > /dev/null
        echo; echo
    fi
}

function launch {
    if [[ "$#" -lt 2  ]]; then
        echo "launch <label> <class> <params...>"
        return 1
    fi

    refresh_cluster

    output="$RESULTS_DIR/$1.json"
    cmd="python run.py $output $PACKAGE_BASE.$2 $TARGET_JAR \
      \"--label $1 --isolationLevel $ISOLATION --optOrNot $IS_OPTIMISTIC \
      --par $TOTAL_SLOTS --partitioning $TOTAL_SLOTS \
      ${@:3} $DEFAULT\""

    opt=OPT
    if [[ $IS_OPTIMISTIC != true ]]; then
        opt=PESS
    fi

    notify "[BEGIN] t-spoon experiment" "$1 $opt-PL$ISOLATION"
    echo $cmd
    status="OK"
    if [[ $DEBUG != true ]]; then
        eval "$cmd"
        exit_code=$?
        if [[ $exit_code -eq 1 ]]; then
          status="ERROR"
        elif [[ $exit_code -eq 2 ]]; then
          status="CANCELED"
        fi
    fi
    notify "[END-$status] t-spoon experiment" "$1 $opt-PL$ISOLATION"
}

function stop-cluster {
    echo; echo ">>> Stopping Flink cluster..."; echo
    sleep 1

    if [[ $DEBUG != true ]]; then
        $FLINK_HOME/bin/stop-cluster.sh
    fi

    echo; echo
}

function start-cluster {
    echo; echo ">>> Starting Flink cluster..."; echo
    sleep 1

    if [[ $DEBUG != true ]]; then
        $FLINK_HOME/bin/start-cluster.sh
    fi

    echo; echo
}

function restart-cluster {
    stop-cluster
    sleep_if $REFRESH_SLEEP
    start-cluster
    sleep_if $REFRESH_SLEEP
}
