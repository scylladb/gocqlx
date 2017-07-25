all: check test

.PHONY: check
check:
	gofmt -s -l . | ifne false
	go vet ./...
	golint -set_exit_status ./...
	misspell ./...
	ineffassign .

.PHONY: test
test:
	go test -race ./...
	go test -tags integration ./...

.PHONY: get-deps
get-deps:
	go get -t ./...

	go get -u github.com/golang/lint/golint
	go get -u github.com/client9/misspell/cmd/misspell
	go get -u github.com/gordonklaus/ineffassign
