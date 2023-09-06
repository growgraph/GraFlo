# how to build Dockerfile

# to run containers from docker compose

```shell
docker compose --env-file .env up <container_a> <container_b> -d
```

# to stop containers from docker compose

```shell
docker compose stop <container_a> <container_b>
```

# to bash into a container

```shell
docker exec -it <container_hash> bash
```


## arangoshell

```shell
arangosh --server.endpoint tcp://127.0.0.1:8529 --server.username <name> --server.database <db_name>
```


## neo4j shell

`cypher-shell` is available in docker, but it is easier to work with [http://localhost:7475](http://localhost:7475). NB: standard neo4j port is `7474`.
