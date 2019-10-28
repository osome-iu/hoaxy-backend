# Build docker image
```ssh
# Python 2.7
docker build -t lucmichalskip/hoaxy-backend:alpine-miniconda2-jdk8 -f miniconda2-alpine3.9.dockerfile .
```

# Pull docker image
```ssh
docker pull lucmichalskip/hoaxy-backend:alpine-miniconda2-jdk8
```

# Run docker container
```ssh
docker -ti --rm lucmichalskip/hoaxy-backend:alpine-miniconda2-jdk8 hoaxy -h
docker run --rm -v $PWD/shared/config:/home/.hoaxy lucmichalskip/hoaxy-backend:alpine-miniconda2-jdk8 hoaxy config
```

# Enter dev container
```
docker run -ti lucmichalskip/hoaxy-backend:alpine-miniconda2-jdk8
docker run -ti -v $PWD/shared/config:/home/.hoaxy lucmichalskip/hoaxy-backend:alpine-miniconda2-jdk8
```

# Create alias
```ssh
alias hoaxy='docker run --rm -v $PWD/shared/config:/home/.hoaxy lucmichalskip/hoaxy-backend:alpine-miniconda2-jdk8 hoaxy'
```

# Create Hoaxy db (without docker-compose):
```
docker run --name hoaxy-psql -itd --restart always \
  --publish 5432:5432 \
  --volume /srv/docker/postgresql:/var/lib/postgresql \
  --env 'PG_TRUST_LOCALNET=true' \
  --env 'DB_USER=hoaxy' --env 'DB_PASS=pizza' \
  --env 'DB_NAME=hoaxy' \
  sameersbn/postgresql:9.6-2
```

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