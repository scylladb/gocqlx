all: test bench

.PHONY: fmt
fmt:
	@go fmt ./...

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
