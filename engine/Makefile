# Makefile for building the Splitgraph engine

export DOCKER_REPO ?= splitgraph
export DOCKER_TAG ?= development
export DOCKER_CACHE_REPO ?= ${DOCKER_REPO}
export DOCKER_CACHE_TAG ?= ${DOCKER_TAG}
export DOCKER_ENGINE_IMAGE ?= engine
export DOCKER_BUILDKIT=1

SHELL=/bin/bash

.PHONY: build toolchain
.DEFAULT_GOAL := build

with_postgis ?= ""


build:
	cd .. && docker build \
	--build-arg BUILDKIT_INLINE_CACHE=1 \
	--build-arg with_postgis=$(with_postgis) \
	-t $$DOCKER_REPO/$$DOCKER_ENGINE_IMAGE:$$DOCKER_TAG \
	--cache-from=$$DOCKER_CACHE_REPO/$$DOCKER_ENGINE_IMAGE:$$DOCKER_CACHE_TAG \
	-f engine/Dockerfile .

toolchain:
	cd .. && docker build \
	--build-arg BUILDKIT_INLINE_CACHE=1 \
	-t $$DOCKER_REPO/engine-toolchain:$$DOCKER_TAG \
	--cache-from=$$DOCKER_CACHE_REPO/$$DOCKER_ENGINE_IMAGE:$$DOCKER_CACHE_TAG \
	--target toolchain -f engine/Dockerfile .

# e.g make target=multicorn intermediate will build just the builder_multicorn stage.
intermediate:
ifndef target
	$(error target is not set)
endif
	cd .. && docker build \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		-t $$DOCKER_REPO/engine-builder-$(target):$$DOCKER_TAG \
		--cache-from=$$DOCKER_CACHE_REPO/$$DOCKER_ENGINE_IMAGE:$$DOCKER_CACHE_TAG \
		--target builder_$(target) -f engine/Dockerfile .
