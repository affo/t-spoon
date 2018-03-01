#!/bin/bash
source voltrc

javac -d $DATA_DIR -cp "$VOLT_HOME/voltdb/*" \
  AsyncBenchmark.java Transfer.java

jar cvf $DATA_DIR/storedprocs.jar -C $DATA_DIR Transfer.class

$VOLT_HOME/bin/sqlcmd < init.sql
