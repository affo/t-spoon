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

function launch {
    if [[ "$#" -lt 1  ]]; then
        echo "launch <class> <params...>"
        return 1
    fi

    cmd="$FLINK_BIN run -c "$PACKAGE_BASE.$1" $TARGET_JAR ${@:2} --isolationLevel $ISOLATION --optOrNot $IS_OPTIMISTIC $DEFAULT"

    echo $cmd
    if [[ $DEBUG != true ]]; then
        eval $cmd
    fi
}

