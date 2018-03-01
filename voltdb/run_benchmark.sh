#!/bin/bash
source voltrc

echo "You can pass arguments in as --opt <value>, see KVConfig in AsyncBenchmark.java."
sleep 2

java -cp "$DATA_DIR:$VOLT_HOME/voltdb/*" \
		AsyncBenchmark "$@"
