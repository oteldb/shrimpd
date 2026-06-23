test:
	@./go.test.sh
.PHONY: test

coverage:
	@./go.coverage.sh
.PHONY: coverage

test_fast:
	go test -short ./...

tidy:
	go mod tidy
