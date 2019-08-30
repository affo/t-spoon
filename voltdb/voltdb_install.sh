#!/bin/bash
source voltrc

javac -d $DATA_DIR -cp "$VOLT_HOME/voltdb/*" AsyncBenchmark.java Transfer.java Deposit.java

jar cvf $DATA_DIR/storedprocs.jar -C $DATA_DIR Transfer.class -C $DATA_DIR Deposit.class

$VOLT_HOME/bin/sqlcmd < init.sql
