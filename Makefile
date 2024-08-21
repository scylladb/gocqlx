all: check test

ifndef SCYLLA_IMAGE
SCYLLA_IMAGE := scylladb/scylla:6.0.0
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

.PHONY: fmt
fmt:
	@go fmt ./...

.PHONY: check
check:
	@$(GOBIN)/golangci-lint run ./...

.PHONY: fix
fix:
	@$(GOBIN)/golangci-lint run --fix ./...
	@fieldalignment -V=full >/dev/null 2>&1 || go install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@v0.11.0
	@$(GOBIN)/fieldalignment -test=false -fix  ./...

GOTEST := go test -cpu $(GOTEST_CPU) -count=1 -cover -race -tags all

.PHONY: test
test:
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

.PHONY: run-scylla
run-scylla:
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

define dl_tgz
	@curl -sSq -L $(2) | tar zxf - --strip 1 -C $(GOBIN) --wildcards '*/$(1)'
endef

.PHONY: get-tools
get-tools:
	@echo "==> Installing tools at $(GOBIN)..."
	@$(call dl_tgz,golangci-lint,https://github.com/golangci/golangci-lint/releases/download/v1.59.1/golangci-lint-1.59.1-linux-amd64.tar.gz)
