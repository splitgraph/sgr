# Makefile for building the Splitgraph engine

export DOCKER_REPO ?= splitgraph
export DOCKER_TAG ?= latest
export DOCKER_ENGINE_IMAGE ?= engine
export DOCKER_BUILDKIT=1

SHELL=/bin/bash

.PHONY: build toolchain
.DEFAULT_GOAL := build

build:
	docker build -t $$DOCKER_REPO/$$DOCKER_ENGINE_IMAGE:$$DOCKER_TAG .

toolchain:
	docker build -t $$DOCKER_REPO/engine-toolchain:$$DOCKER_TAG --target toolchain .
