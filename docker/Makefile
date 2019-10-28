DOCKER_USERNAME := lucmichalskip
DOCKER_NAME := hoaxy-backend
DOCKER_TAG := miniconda3-jdk-alpine3.9
DOCKER_IMAGE := "$(DOCKER_USERNAME)/$(DOCKER_NAME):$(DOCKER_TAG)"

docker-all: docker-build docker-init

docker-build:
	@docker build -t $(DOCKER_IMAGE) .

docker-init:
	@docker run --rm $(DOCKER_IMAGE) config
