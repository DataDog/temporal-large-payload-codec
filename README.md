# Large Payload Service

This repository contains an implementation of the Temporal [Payload Codec](https://docs.temporal.io/concepts/what-is-a-data-converter/#payload-codecs).
[Temporal](https://temporal.io/) limits payload size to 4MB.
This Payload Codec allows you to use payloads larger than this limit by transparently storing and retrieving these payloads to and from cloud storage.

In essence, the Large Payload Service implements a [content addressed storage](https://en.wikipedia.org/wiki/Content-addressable_storage) system (CAS) for Temporal workflow payloads.

<!-- toc -->

- [API](#api)
- [Usage](#usage)
- [Development](#development)

<!-- tocstop -->

## API

Architecturally, the Large Payload Service is an HTTP server offering the following API:

- `/v2/health/head`: Health check endpoint using a `HEAD` request.
   
   Returns the HTTP response status code 200 if the service is running correctly. 
   Otherwise, an error code is returned.

- `/v2/blobs/put`: Upload endpoint expecting a `PUT` request.

   **Required headers**:
   - `Content-Type` set to `application/octet-stream`.
   - `Content-Length` set to the length of payload the data in bytes.
   - `X-Temporal-Metadata` set to the base64 encoded JSON of the Temporal Metadata.

   **Query parameters**:
   - `namespace` The Temporal namespace the client using the codec is connected to.
  
     The namespace forms part of the key for retrieval of the payload.
   - `digest` Specifies the checksum over the payload data using the format `sha256:<sha256_hex_encoded_value>`.

   The returned _key_ of the put request needs to be stored and used for later retrieval of the payload.
   It is up to the Large Payload Server and the backend driver how to arrange the data in the backing data store.
   The server will honor, however, the value of `remote-codec/key-prefix` in the Temporal Metadata passed via the `X-Temporal-Metadata` header.
   It will use the specified string as prefix in the storage path.

- `/v2/blobs/get`: Download endpoint expecting a `GET` request.

  **Required headers**:
  - `Content-Type` set to `application/octet-stream`.
  - `X-Payload-Expected-Content-Length` set to the expected size of the payload data in bytes.

  **Query parameters**:
  - `key` specifying the key for the payload to retrieve.


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
    bucket, set := os.LookupEnv("BUCKET")
    if !set {
        log.Fatal("BUCKET environment variable not set")
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

Refer to [CONTRIBUTING.md](./CONTRIBUTING.md) for instructions on how to build and test the Large Payload Service and for general contributing guidelines.
