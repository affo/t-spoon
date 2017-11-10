if [[ -e "launchrc" ]]; then
    source launchrc
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

function launch {
    if [[ "$#" -lt 1  ]]; then
        echo "launch <class> <params...>"
        return 1
    fi

    refresh_cluster

    cmd="$FLINK_BIN run -c "$PACKAGE_BASE.$1" $TARGET_JAR ${@:2} --isolationLevel $ISOLATION --optOrNot $IS_OPTIMISTIC $DEFAULT"

    echo $cmd
    if [[ $DEBUG != true ]]; then
        eval $cmd
    fi
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
    sleep_if 5
    start-cluster
    sleep_if 5
}

