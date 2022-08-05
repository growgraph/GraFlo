#!/bin/bash

if [ $# -ne 2 ]; then
        echo "please specify 2 command line arguments"
		exit 1
fi

credname=$1
credpass=$2

python run/arango/ingest_csv.py --config-path ./conf/ibes.yaml --path ~/data/investing/ibes/main/ --db finance --clean-start --cred-name "$credname" --cred-pass "$credpass"
python run/arango/ingest_csv.py --config-path ./conf/ticker.yaml --path ~/data/investing/yahoo/history/ --db finance --cred-name "$credname" --cred-pass "$credpass"