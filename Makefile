.PHONY: default all image deploy
THIS_FILE := $(lastword $(MAKEFILE_LIST))

BUILD_TARGET ?= tzindex
BUILD_VERSION ?= $(shell git describe --always --dirty)
BUILD_COMMIT ?= $(shell git rev-parse --short HEAD)
BUILD_IMAGE := blockwatch/$(BUILD_TARGET):$(BUILD_VERSION)
BUILD_LATEST := blockwatch/$(BUILD_TARGET):latest
export BUILD_TARGET BUILD_VERSION BUILD_COMMIT BUILD_IMAGE BUILD_LATEST

BUILD_FLAGS := --build-arg BUILD_TARGET=$(BUILD_TARGET) --build-arg BUILD_COMMIT=$(BUILD_COMMIT) --build-arg BUILD_VERSION=$(BUILD_VERSION)

all: image

image:
	@echo $@
	docker build -f docker/Dockerfile --pull --no-cache --rm --tag $(BUILD_IMAGE) --tag $(BUILD_LATEST) $(BUILD_FLAGS) .
	docker image prune --force --filter "label=autodelete=true"

deploy: image
	@echo $@
	@echo "Publishing image..."
	docker login -u $(DOCKER_REGISTRY_USER) -p $(DOCKER_REGISTRY_PASSPHRASE) $(DOCKER_REGISTRY_ADDR)
	docker push $(BUILD_IMAGE)
	docker push $(BUILD_LATEST)
