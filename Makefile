.PHONY: default all image deploy
THIS_FILE := $(lastword $(MAKEFILE_LIST))

FLAVOR ?= alpine
REPO ?= blockwatch.cc/tzindex
BUILD_TARGET ?= tzindex
BUILD_VERSION ?= $(shell git describe --always --tags --dirty)
BUILD_COMMIT ?= $(shell git rev-parse --short HEAD)
BUILD_IMAGE := blockwatch/$(BUILD_TARGET):$(BUILD_VERSION)
BUILD_LATEST := blockwatch/$(BUILD_TARGET):latest
BUILD_TAG ?= master
export BUILD_TAG BUILD_TARGET BUILD_VERSION BUILD_COMMIT BUILD_IMAGE BUILD_LATEST

BUILD_FLAGS := --build-arg BUILD_TARGET=$(BUILD_TARGET) --build-arg BUILD_COMMIT=$(BUILD_COMMIT) --build-arg BUILD_VERSION=$(BUILD_VERSION) --build-arg BUILD_TAG=$(BUILD_TAG)

all: build

build:
	@echo $@
	go clean
	go mod download
	CGO_ENABLED=0 go build -a -o ./ -ldflags "-w -X ${REPO}/cmd.VERSION=${BUILD_VERSION} -X ${REPO}/cmd.GITCOMMIT=${BUILD_COMMIT}" ./cmd/...

image:
	@echo $@
	docker build -f docker/Dockerfile.${FLAVOR} --pull --no-cache --rm --tag $(BUILD_IMAGE) --tag $(BUILD_LATEST) $(BUILD_FLAGS) .
	docker image prune --force --filter "label=autodelete=true"

deploy: image
	@echo $@
	@echo "Publishing image..."
	docker login -u $(DOCKER_REGISTRY_USER) -p $(DOCKER_REGISTRY_PASSPHRASE) $(DOCKER_REGISTRY_ADDR)
	docker push $(BUILD_IMAGE)
	docker push $(BUILD_LATEST)
