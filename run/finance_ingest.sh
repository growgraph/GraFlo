#!/bin/bash

if [ $# -ne 3 ]; then
        echo "please specify 3 command line arguments"
		exit 1
fi

credname=$1
credpass=$2
mainpath=$3
ibespath="$mainpath/ibes/main/"
yahoopath="$mainpath/yahoo/history/"



python ./arango/ingest_csv.py --config-path ../conf/ibes.yaml --path "$ibespath" --db finance --clean-start --cred-name "$credname" --cred-pass "$credpass"
python ./arango/ingest_csv.py --config-path ../conf/ticker.yaml --path "$yahoopath" --db finance --cred-name "$credname" --cred-pass "$credpass"