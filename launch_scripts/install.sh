#!/bin/bash

if [[ -d "./venv"  ]]; then
    echo "Virtualenv found, removing..."
    rm -r venv
fi

virtualenv venv
source venv/bin/activate
# reinstall pip for TLS problems
curl https://bootstrap.pypa.io/get-pip.py | python # re
pip install -r requirements.txt

