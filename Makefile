build:
	go build -o lps ./server/cmd/main.go

test_codec:
	go test ./codec/...

test_server:
	go test ./server/...

test: test_codec test_server

format:
	gofmt -l -w .

update_license_file:
	go run ./internal/tools/licensecheck

update_copyright_headers:
	go run ./internal/tools/copyright