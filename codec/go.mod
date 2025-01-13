module github.com/DataDog/temporal-large-payload-codec/codec

go 1.19

replace github.com/DataDog/temporal-large-payload-codec/server => ../server

require (
	github.com/DataDog/temporal-large-payload-codec/server v1.3.9
	github.com/stretchr/testify v1.8.0
	go.temporal.io/api v1.8.1-0.20220603192404-e65836719706
	go.temporal.io/sdk v1.15.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/net v0.10.0 // indirect
	golang.org/x/sys v0.8.0 // indirect
	golang.org/x/text v0.9.0 // indirect
	google.golang.org/genproto v0.0.0-20220815135757-37a418bb8959 // indirect
	google.golang.org/grpc v1.48.0 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
