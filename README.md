Python package for casting csv, json structured data 
 to graphs, amenable for ingestion by graph databases, e.g. ArangoDB.

---

## Development

To install requirements

```shell
poetry install --group dev
pre-commit install
```


## Tests

To run unit tests

```shell
make test
```

NB: spin up Arango in [docker folder](./docker/arango) by

```shell
docker-compose --env-file .env up arango
```


