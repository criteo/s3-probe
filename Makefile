SHELL = /usr/bin/env bash -o pipefail

IMG ?= s3-probe:latest

GOPKGS ?= ./pkg/...
GOPKG = $(shell go list -m -f '{{ .Path }}')
GOPKGS_DIRS := $(shell go list -f '{{ .Dir }}' $(GOPKGS))

IMG ?= s3-probe:latest

##@ Development

.PHONY: deps
deps:
	go mod tidy
	go mod vendor

.PHONY: fmt
fmt:
	@out=$$(gofmt -d -e -s ${GOPKGS_DIRS} 2>&1) && [ -z "$${out}" ]  || (echo "$${out}";exit 1)

.PHONY: update-fmt
update-fmt:
	@gofmt -l -w ${GOPKGS_DIRS}

.PHONY: vet
vet:
	@go vet ${GOPKGS}

.PHONY: test
test: fmt vet

.PHONY: update-go-deps
update-go-deps:
	@for m in $$(go list -mod=readonly -m -f '{{ if and (not .Indirect) (not .Main)}}{{.Path}}{{end}}' all); do \
		go get $$m; \
	done
	go mod tidy
	go mod vendor

##@ Build

.PHONY: run
run: test
	go run ./pkg/cmd/...

.PHONY: build
build: test
	GOOS=linux go build -v -o bin/s3-probe ./pkg/cmd/...

.PHONY: image
image: build
	docker build -t ${IMG} .

.PHONY: clean
clean:
	rm -f bin/*