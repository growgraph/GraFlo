Python package for casting csv, json structured data 
 to vertices and edges, amenable for ingestion by graph databases, e.g. ArangoDB.

Installation
------------

To install requirements use
``poetry install``.

Remarks
-------

To test csv schema for WoS

```console
foo@bar:~$ python run/arango/ingest_csv.py --config-path ./conf/wos.yaml --path ./test/data/wos --db wos_test --cred-pass db_password --cred-name db_login
``` 

To test csv schema for IBES

```console
foo@bar:~$ python run/arango/ingest_csv.py --config-path ./conf/ibes.yaml --path ./test/data/ibes --db ibes_test --cred-pass db_password --cred-name db_login
``` 



