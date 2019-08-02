#!/bin/bash

usage() {
    echo "Usage:"
    echo "$0 <identity_file>.pem <jobmanager_IP>"
}

if [[ $# -lt 2 ]]; then
    usage
    exit 1
fi

dest=~/.ssh/config_flink

cat > $dest << EOF
Host jm
  HostName $2
  User ec2-user
  Port 22
  IdentityFile $1
EOF

echo ">>> $dest updated:"
cat $dest
echo "You can now \`ssh jm\`!"
