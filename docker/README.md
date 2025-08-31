# how to build Dockerfile

# to run containers from docker compose

```shell
docker compose --env-file .env up <container_spec> -d
```

# to stop containers from docker compose

```shell
docker compose stop <container_name> 
```

# to bash into a container

```shell
docker exec -it <containter_name sh
```


## arangoshell

Arango web interface [http://localhost:ARANGO_PORT](http://localhost:8535). NB: the standard arango port is 8529, `.env` config in graflo uses 8535.


## neo4j shell

Neo4j web interface [http://localhost:NEO4J_PORT](http://localhost:7475). NB: the standard neo4j port is 7474, `.env` config in graflo uses 7475.
