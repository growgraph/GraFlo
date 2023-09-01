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