all: check test bench

ifndef GOBIN
export GOBIN := $(GOPATH)/bin
endif

.PHONY: fmt
fmt: ## Format source code
	@go fmt ./...

.PHONY: check
check: ## Perform static code analysis
check: .check-misspell .check-lint

.PHONY: .check-fmt
.check-fmt:
	@go fmt ./... | tee /dev/stderr | ifne false

.PHONY: .check-misspell
.check-misspell:
	@$(GOBIN)/misspell ./...

.PHONY: .check-lint
.check-lint:
	@$(GOBIN)/golangci-lint run -s --disable-all -E govet -E errcheck -E staticcheck \
	-E gas -E typecheck -E unused -E structcheck -E varcheck -E ineffassign -E deadcode \
	-E gofmt -E golint -E gosimple -E unconvert -E dupl -E depguard -E gocyclo \
	--tests=false \
	--exclude-use-default=false \
	--exclude='composite literal uses unkeyed fields' \
	--exclude='Error return value of `.+\.Close` is not checked' \
	--exclude='G104' \
	--exclude='G304' \
	--exclude='G401' \
	--exclude='G501' \
	./...

GOTEST := go test -cover -race -tags all

.PHONY: test
test: ## Run unit and integration tests
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

.PHONY: get-tools
get-tools: GOPATH := $(shell mktemp -d)
get-tools:
	@echo "==> Installing tools at $(GOBIN)..."
	@mkdir -p $(GOBIN)

	@go get -u github.com/client9/misspell/cmd/misspell
	@go get -u github.com/golangci/golangci-lint/cmd/golangci-lint

	@rm -Rf $(GOPATH)
