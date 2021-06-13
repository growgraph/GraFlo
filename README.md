Python package for casting csv, json structured data 
 to vertices and edges, amenable for ingestion by graph databases, e.g. ArangoDB.

Installation
------------

To install the requirement use
``poetry install``.

Remarks
-------

To test csv schema 

```console
foo@bar:~$ python run/arango/ingest_csv.py --config-path ./conf/ibes.yaml --path ./test/data/ibes --db ibes_test --cred-pass db_password --cred-pass db_password --cred-name db_login
``` 




