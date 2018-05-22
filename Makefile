all: check test integration-test

.PHONY: check
check: .check-fmt .check-vet .check-lint .check-ineffassign .check-mega .check-misspell

.PHONY: .check-fmt
.check-fmt:
	@gofmt -s -l . | ifne false

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

.PHONY: test
test:
	@go test -cover -race ./...

.PHONY: integration-test
integration-test:
	@go test -cover -tags integration .

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
