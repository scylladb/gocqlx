all: check test integration-test

.PHONY: check
check: .check-fmt .check-vet .check-lint .check-misspell .check-ineffassign

.PHONY: .check-fmt
.check-fmt:
	@gofmt -s -l . | ifne false

.PHONY: .check-vet
.check-vet:
	@go vet ./...

.PHONY: .check-lint
.check-lint:
	@golint -set_exit_status ./...

.PHONY: .check-misspell
.check-misspell:
	@misspell ./...

.PHONY: .check-ineffassign
.check-ineffassign:
	@ineffassign .

.PHONY: test
test:
	@go test -cover -race ./...

.PHONY: integration-test
integration-test:
	@go test -cover -tags integration .

.PHONY: get-deps
get-deps:
	go get -t ./...

	go get -u github.com/golang/lint/golint
	go get -u github.com/client9/misspell/cmd/misspell
	go get -u github.com/gordonklaus/ineffassign
