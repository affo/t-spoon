#!/bin/bash

kill `cat .voltdb_server/server.pid` # Kills some voltDB process if it is up
cd voltdb # set it to VOLTDB_HOME
# launches voltdb in background
./voltdb_deploy.sh 10 leader 8 -B # adapt parameters to your cluster
