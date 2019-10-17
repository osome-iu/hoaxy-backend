# Build docker image
```ssh
docker-compose up
```

# Run the container
Once the build completes, you can run docker command to get the container information. 
```ssh
docker ps -a 
2a4723abf38d        hoaxy-backend:miniconda2-jdk8-alpine3.9   "/opt/entrypoint.sh …"   31 minutes ago      Exited (1) 30 minutes ago                                docker_hoaxy_1
fd683f81e369        postgres:9.6-alpine                       "docker-entrypoint.s…"   31 minutes ago      Up 30 minutes                   0.0.0.0:5432->5432/tcp   docker_postgres_1
d7ce7f3e37df        adminer                                   "entrypoint.sh docke…"   31 minutes ago      Up 30 minutes                   0.0.0.0:8080->8080/tcp   docker_adminer_1

```

Then to connect to hoaxy-backend docker, run
```ssh
docker run -ti  hoaxy-backend:miniconda2-jdk8-alpine3.9 bash
```

Once you are inside docker, you can run
```ssh
hoaxy config
```
to set up hoaxy configs. 
 
# Notes
- Clean up dockerfiles

# To do
- Can be optimized to less than 1Go
- Create a miniconda3 version for next versions
- Why not use a local elk stack instead of pylucene
- Add notes about Makefile commands
- Cleanup this README.md file and format sections

# References
## Docker
### Hoaxy
- https://github.com/ppope/dockerfiles/tree/master/hoaxy
### Miniconda
- https://github.com/frol/docker-alpine-miniconda3 (Alpine 3.9)
- https://github.com/frol/docker-alpine-miniconda2 (Alpine 3.9)
### PostgresQL
- https://github.com/docker-library/postgres/blob/master/9.6/alpine/docker-entrypoint.sh
- https://github.com/autocase/Alpine-uWSGI-PostgreSQL/blob/master/Dockerfile#L8
- https://github.com/autocase/Alpine-uWSGI-PostgreSQL-SciPy-NumPy/blob/master/Dockerfile#L8
### Sidekick
- https://github.com/librariesio/libraries.io/blob/master/docker-compose.yml
### Security
- https://github.com/krallin/tini/releases (A tiny but valid `init` for containers)
- https://github.com/tianon/gosu/releases (Simple Go-based setuid+setgid+setgroups+exec)