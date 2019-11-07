all: check test

ifndef GOBIN
export GOBIN := $(GOPATH)/bin
endif

define dl_tgz
	@curl -sSq -L $(2) | tar zxf - --strip 1 -C $(GOBIN) --wildcards '*/$(1)'
endef

.PHONY: fmt
fmt:
	@go fmt ./...

.PHONY: check
check:
	@$(GOBIN)/golangci-lint run ./...

GOTEST := go test -cover -race -tags all

.PHONY: test
test:
	@$(GOTEST) .
	@$(GOTEST) ./migrate
	@$(GOTEST) ./qb
	@$(GOTEST) ./table

.PHONY: bench
bench:
	@go test -tags all -run=XXX -bench=. -benchmem ./...

.PHONY: get-deps
get-deps:
	go get -t ./...

.PHONY: get-tools
get-tools:
	@echo "==> Installing tools at $(GOBIN)..."
	@$(call dl_tgz,golangci-lint,https://github.com/golangci/golangci-lint/releases/download/v1.21.0/golangci-lint-1.21.0-linux-amd64.tar.gz)
