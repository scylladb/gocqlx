all: check test bench

.PHONY: check
check: .check-fmt .check-vet .check-lint .check-ineffassign .check-mega .check-misspell

.PHONY: .check-fmt
.check-fmt:
	@go fmt ./... | tee /dev/stderr | ifne false

.PHONY: .check-vet
.check-vet:
	@go vet ./...

.PHONY: .check-lint
.check-lint:
	@golint -set_exit_status ./...

.PHONY: .check-ineffassign
.check-ineffassign:
	@ineffassign .

.PHONY: .check-mega
.check-mega:
	@megacheck ./...

.PHONY: .check-misspell
.check-misspell:
	@misspell ./...

GOTEST := go test -cover -race -tags all

.PHONY: test
test:
	@$(GOTEST) .
	@$(GOTEST) ./migrate
	@$(GOTEST) ./qb
	@$(GOTEST) ./reflectx

.PHONY: bench
bench:
	@go test -tags all -run=XXX -bench=. -benchmem ./...

.PHONY: get-deps
get-deps:
	go get -t ./...

ifndef GOBIN
export GOBIN := $(GOPATH)/bin
endif

.PHONY: get-tools
get-tools: GOPATH := $(shell mktemp -d)
get-tools:
	@echo "==> Installing tools at $(GOBIN)..."
	@mkdir -p $(GOBIN)

	@go get -u github.com/golang/lint/golint
	@go get -u github.com/client9/misspell/cmd/misspell
	@go get -u github.com/gordonklaus/ineffassign
	@go get -u honnef.co/go/tools/cmd/megacheck

	@rm -Rf $(GOPATH)
