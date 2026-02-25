GOLANGCI_LINT_VERSION ?= v2.10.1

build:
	go build -o lps ./server/cmd/main.go

test_codec:
	go test ./codec/...

test_server:
	go test ./server/...

test: test_codec test_server

format:
	gofmt -l -w .

lint: install-tools
	cd codec && golangci-lint run ./...
	cd server && golangci-lint run ./...

update_license_file:
	go run ./internal/tools/licensecheck

check_license:
	go run ./internal/tools/licensecheck
	@if ! git diff --quiet LICENSE-3rdparty.csv; then \
		echo "LICENSE-3rdparty.csv is out of date. Please run 'make update_license_file' and commit the changes."; \
		exit 1; \
	fi

update_copyright_headers:
	go run ./internal/tools/copyright

check_copyright:
	@if ! go run ./internal/tools/copyright -verifyOnly; then \
		echo "Required Go header(s) Missing. Please run 'make update_copyright_headers' and commit the changes."; \
		exit 1; \
	fi

install-tools:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
