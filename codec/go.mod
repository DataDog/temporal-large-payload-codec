module github.com/DataDog/temporal-large-payload-codec/codec

go 1.25.6

replace github.com/DataDog/temporal-large-payload-codec/server => ../server

require (
	github.com/DataDog/temporal-large-payload-codec/server v1.3.1
	github.com/golang/protobuf v1.5.4
	github.com/stretchr/testify v1.11.1
	go.temporal.io/api v1.62.2
	go.temporal.io/sdk v1.40.0
)

require (
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.2 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	golang.org/x/net v0.50.0 // indirect
	golang.org/x/sys v0.41.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260223185530-2f722ef697dc // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260223185530-2f722ef697dc // indirect
	google.golang.org/grpc v1.79.1 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
