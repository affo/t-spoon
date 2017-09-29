#!/bin/bash

echo "172.31.31.196 jobmanager" | sudo tee -a /etc/hosts

# this gist contains the configuration for flink
wget https://gist.github.com/affo/56de309bdbcf92720d69fbbeed31bd92/raw -O flink-conf.yaml
mv flink-conf.yaml /home/ec2-user/flow-workspace/flink-1.3.2/conf/
java_version=$(java -version 2>&1 | head -1 | awk -F '"' '{print $2}')

if [[ $version != 1.8*  ]]; then
    echo "Installing Java 8 and removing 7..."
    # TODO should remove the right version...
    sudo yum install -y java-1.8.0 && sudo yum remove -y java-1.7.0-openjdk
fi

echo; echo; echo
java -version
echo; echo; echo

