all: check test

ifndef SCYLLA_IMAGE
SCYLLA_IMAGE := scylladb/scylla
endif

ifndef SCYLLA_CPU
SCYLLA_CPU := 0
endif

ifndef GOTEST_CPU
GOTEST_CPU := 1
endif

ifndef GOPATH
GOPATH := $(shell go env GOPATH)
endif

ifndef GOBIN
GOBIN := $(GOPATH)/bin
endif
export PATH := $(GOBIN):$(PATH)

GOOS := $(shell uname | tr '[:upper:]' '[:lower:]')
GOARCH := $(shell go env GOARCH)

GOLANGCI_VERSION := 1.64.8
FIELDALIGNMENT_VERSION := 0.24.0

ifeq ($(GOARCH),arm64)
	GOLANGCI_DOWNLOAD_URL := "https://github.com/golangci/golangci-lint/releases/download/v$(GOLANGCI_VERSION)/golangci-lint-$(GOLANGCI_VERSION)-$(GOOS)-arm64.tar.gz"
else ifeq ($(GOARCH),amd64)
	GOLANGCI_DOWNLOAD_URL := "https://github.com/golangci/golangci-lint/releases/download/v$(GOLANGCI_VERSION)/golangci-lint-$(GOLANGCI_VERSION)-$(GOOS)-amd64.tar.gz"
else
	@printf 'Unknown architecture "%s"\n', "$(GOARCH)"
	@exit 69
endif

.PHONY: fmt
fmt:
	@go fmt ./...

.PHONY: check
check: .require-golangci-lint
	@golangci-lint run ./...

.PHONY: fix
fix: .require-golangci-lint .require-fieldalignment
	@$(MAKE) fmt
	@golangci-lint run --fix ./...
	@fieldalignment -test=false -fix  ./...

GOTEST := go test -cpu $(GOTEST_CPU) -count=1 -cover -race -tags all

.PHONY: test
test: start-scylla
	echo "==> Running tests..."
	echo "==> Running tests... in ."
	@$(GOTEST) .
	echo "==> Running tests... in ./qb"
	@$(GOTEST) ./qb
	echo "==> Running tests... in ./table"
	@$(GOTEST) ./table
	echo "==> Running tests... in ./migrate"
	@$(GOTEST) ./migrate
	echo "==> Running tests... in ./dbutil"
	@$(GOTEST) ./dbutil
	echo "==> Running tests... in ./cmd/schemagen"
	@$(GOTEST) ./cmd/schemagen
	echo "==> Running tests... in ./cmd/schemagen/testdata"
	@cd ./cmd/schemagen/testdata ; go mod tidy ; $(GOTEST) .; cd ../../..

.PHONY: bench
bench:
	@go test -cpu $(GOTEST_CPU) -tags all -run=XXX -bench=. -benchmem ./...

.PHONY: run-examples
run-examples:
	@go test -tags all -v -run=Example ./...

.PHONY: start-scylla
start-scylla:
	@echo "==> Running test instance of Scylla $(SCYLLA_IMAGE)"
	@docker pull $(SCYLLA_IMAGE)
	@docker run --name gocqlx-scylla -p 9042:9042 --cpuset-cpus=$(SCYLLA_CPU) --memory 1G --rm -d $(SCYLLA_IMAGE)
	@until docker exec gocqlx-scylla cqlsh -e "DESCRIBE SCHEMA"; do sleep 2; done

.PHONY: stop-scylla
stop-scylla:
	@docker stop gocqlx-scylla

.PHONY: get-deps
get-deps:
	@go mod download

.PHONY: get-tools
get-tools:
	@echo "==> Installing tools at $(GOBIN)..."
	@$(MAKE) install-golangci-lint
	@$(MAKE) install-fieldalignment

.require-golangci-lint:
ifeq ($(shell if golangci-lint --version 2>/dev/null | grep ${GOLANGCI_VERSION} 1>/dev/null 2>&1; then echo "ok"; else echo "need-install"; fi), need-install)
	$(MAKE) install-golangci-lint
endif

install-golangci-lint:
	@echo "==> Installing golangci-lint ${GOLANGCI_VERSION} at $(GOBIN)..."
	$(call dl_tgz,golangci-lint,$(GOLANGCI_DOWNLOAD_URL))

.require-fieldalignment:
ifeq ($(shell if fieldalignment -V=full 1>/dev/null 2>&1; then echo "ok"; else echo "need-install"; fi), need-install)
	$(MAKE) install-golangci-lint
endif

install-fieldalignment:
	@echo "==> Installing fieldalignment ${FIELDALIGNMENT_VERSION} at $(GOBIN)..."
	@go install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@v${FIELDALIGNMENT_VERSION}

define dl_tgz
	@mkdir "$(GOBIN)" 2>/dev/null || true
	@echo "Downloading $(GOBIN)/$(1)";
	@curl --progress-bar -L $(2) | tar zxf - --wildcards --strip 1 -C $(GOBIN) '*/$(1)';
	@chmod +x "$(GOBIN)/$(1)";
endef
