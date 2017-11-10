#!/bin/bash

port=${1:-2121}

twistd -n ftp -r ./results/ -p $port
