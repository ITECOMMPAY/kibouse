GOFMT=gofmt
ALL_SRC:=$(shell find . -name "*.go" | \
	grep -v \
		-e vendor \
	)

.PHONY: build
build: dep ## Build the binary
	@go build -i -v -o ./bin/kibouse ./main.go

.PHONY: build-linux
build-linux: dep ## Build the binary
	CGO_ENABLED=0 GOOS=linux installsuffix=cgo go build -o ./bin/kibouse-linux ./main.go

.PHONY: dep
dep: ## Get the dependencies
	go get -v -d ./...

.PHONY: fmt
fmt:
	./scripts/import-order-cleanup.sh inplace
	$(GOFMT) -e -s -l -w $(ALL_SRC)

help: ## Display this help screen
	grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'