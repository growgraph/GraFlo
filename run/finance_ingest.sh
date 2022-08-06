#!/bin/bash

if [ $# -ne 2 ]; then
        echo "please specify db-config-path and data-path"
		exit 1
fi

confpath=$1
mainpath=$2
ibespath="$mainpath/ibes/main/"
yahoopath="$mainpath/yahoo/history/"



python ./arango/ingest_csv.py --config-path ../conf/ibes.yaml --path "$ibespath" --db-config-path "$confpath"
python ./arango/ingest_csv.py --config-path ../conf/ticker.yaml --path "$yahoopath" --db-config-path "$confpath"