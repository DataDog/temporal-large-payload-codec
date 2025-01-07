module github.com/DataDog/temporal-large-payload-codec/codec

go 1.21

toolchain go1.23.4

replace github.com/DataDog/temporal-large-payload-codec/server v1.3.0 => ../server

require (
	github.com/DataDog/temporal-large-payload-codec/server v1.3.0
	github.com/stretchr/testify v1.10.0
	go.temporal.io/api v1.43.0
	go.temporal.io/sdk v1.31.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/net v0.28.0 // indirect
	golang.org/x/sys v0.24.0 // indirect
	golang.org/x/text v0.17.0 // indirect
	google.golang.org/genproto v0.0.0-20220815135757-37a418bb8959 // indirect
	google.golang.org/grpc v1.66.0 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
