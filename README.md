# Large Payload Service

This repository contains an implementation of the Temporal [Payload Codec](https://docs.temporal.io/concepts/what-is-a-data-converter/#payload-codecs).
[Temporal](https://temporal.io/) limits payload size to 4MB.
This Payload Codec allows you to use payloads larger than this limit by transparently storing and retrieving these payloads to and from cloud storage.

In essence, the Large Payload Service implements a [content addressed storage](https://en.wikipedia.org/wiki/Content-addressable_storage) system (CAS) for Temporal workflow payloads.

<!-- toc -->

- [Large Payload Service](#large-payload-service)
  - [Architecture](#architecture)
  - [Usage](#usage)
  - [Development](#development)
    - [Build the Source](#build-the-source)
    - [Run the Tests](#run-the-tests)
    - [Format the Code](#format-the-code)

<!-- tocstop -->

## Architecture

Architecturally, the Large Payload Service is an HTTP server offering the following API:

- `/v1/health/head`: Health check endpoint. Returns the HTTP response status code 200 if the service is running correctly. Otherwise, an error code is returned.
- `/v1/blobs/put`: Upload endpoint expecting a `PUT` request with the `Content-Type` header of `application/octet-stream`.
The endpoint also expects a `digest` query parameter, specifying the key under which to store the sent data.
The `Content-Length` needs to specify the length of the data in bytes, and the `X-Temporal-Metadata` needs to specify a base64 encoded JSON string.
- `/v1/blobs/get`: Download endpoint expecting a `GET` request with the `Content-Type` header of `application/octet-stream`.
The endpoint also expects a `digest` query parameter, specifying the key for the data to retrieve.
The `X-Payload-Expected-Content-Length` header needs to be set to the expected size of the retrieved data in bytes.

## Usage

This repository does not provide any prebuilt binaries or images.
The recommended approach is to build your own binary and image.

To programmatically build the Large Payload Service, you need to instantiate the driver and then pass it to `server.NewHttpHandler`.
For example, to create a Large Payload Service instance backed by an S3 bucket, you would do something along these lines:

```golang
package main

import (
    "context"
    "net/http"
    "os"

    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/DataDog/temporal-large-payload-codec/server"
    "github.com/DataDog/temporal-large-payload-codec/server/storage/s3"
    ...
)

func main() {
    region, set := os.LookupEnv("AWS_SDK_REGION")
    if !set {
        log.Fatal("AWS_REGION environment variable not set")
    }
    bucket, set := os.LookupEnv("S3_BUCKET")
    if !set {
        log.Fatal("S3_BUCKET environment variable not set")
    }

    cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
    if err != nil {
        log.Fatal(err)
    }

    driver := s3.New(&s3.Config{
      Config: cfg,
      Bucket: bucket,
    })

    mux := http.NewServeMux()
    mux.Handle("/", server.NewHttpHandler(driver)))

    if err := http.ListenAndServe(":8577", mux); err != nil {
      log.Fatal(err)
    }
}
```

On the Temporal side, you need to set the option when creating the options for your Temporal client (simplified, without error handling):

```golang
opts := client.Options{
...
}

lpsEndpoint, _ := os.LookupEnv("LARGE_PAYLOAD_SERVICE_URL");
lpc, _ := largepayloadcodec.New(largepayloadcodec.WithURL(lpsEndpoint))
opts.DataConverter = converter.NewCodecDataConverter(opts.DataConverter, c)

temporalClient, _ := router.NewClient(opts)
```

## Development

Prerequisite for developing on this code base is an installed [Golang 1.18](https://go.dev/doc/install) environment.

### Build the Source

```sh
go build -o lps cmd/server/main.go
```

### Run the Tests

To run the unit tests:

```sh
go test ./...
```

### Format the Code

```sh
gofmt -l -w .
```
