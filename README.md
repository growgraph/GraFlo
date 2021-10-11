Python package for casting csv, json structured data 
 to vertices and edges, amenable for ingestion by graph databases, e.g. ArangoDB.

Installation
------------

To install requirements use
``poetry install``.

Remarks
-------

To test csv schemas 

```console
foo@bar:~$ python -m unittest test.arango.test_ingest_csv
```
NB: collections `wos_test`, `ibes_test` and `ticker_test` should be created on your instance of ArangoDB.
 

To test json schemas 

```console
foo@bar:~$ python -m unittest test.arango.test_ingest_json
``` 

Full ingestion
---

To do a full ingestion

```
python run/arango/ingest_csv.py --config-path ./conf/ibes.yaml --path ibes_path --db ibes --cred-pass db_password --cred-name db_login
```