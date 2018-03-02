#!/bin/bash
source voltrc

script=remote_deploy.sh

for server in ${SERVERS[@]}; do
  echo ">>> HOST $server"
  ssh $server 'bash -s' < $script
  echo; echo;
  sleep 1
done
