// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/DataDog/temporal-large-payload-codec/logging"
	"github.com/DataDog/temporal-large-payload-codec/server/storage"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/memory"
	"github.com/pkg/errors"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"

	"github.com/DataDog/temporal-large-payload-codec/server"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/gcs"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/s3"
)

var (
	logger = logging.NewBuiltinLogger()
)

func main() {
	driverName := flag.String("driver", "memory", "name of the storage driver [memory|s3]")
	port := flag.Int("port", 8577, "server port")

	flag.Parse()

	ctx := context.Background()
	driver, err := createDriver(ctx, *driverName)
	if err != nil {
		log.Fatal(err)
	}

	validatable, ok := driver.(storage.Validatable)
	if ok {
		err := validatable.Validate(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}

	httpHandler := server.NewHttpHandlerWithLogger(driver, logger)

	logger.Info(fmt.Sprintf("starting server on port %d", *port))
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *port), httpHandler); err != nil {
		log.Fatal(err)
	}
}

func createDriver(ctx context.Context, driverName string) (storage.Driver, error) {
	var driver storage.Driver

	normalizedDriverName := strings.ToLower(driverName)
	switch normalizedDriverName {
	case "memory":
		logger.Info(fmt.Sprintf("creating %s driver", driverName))
		driver = &memory.Driver{}
	case "s3":
		logger.Info(fmt.Sprintf("creating %s driver", driverName))
		region, set := os.LookupEnv("AWS_REGION")
		if !set {
			return nil, errors.New("AWS_REGION environment variable not set")
		}
		bucket, set := os.LookupEnv("BUCKET")
		if !set {
			return nil, errors.New("BUCKET environment variable not set")
		}

		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
		if err != nil {
			return nil, err
		}

		driver = s3.New(&s3.Config{
			Config: cfg,
			Bucket: bucket,
		})
	case "gcs":
		log.Printf("creating %s driver", driverName)
		bucket, set := os.LookupEnv("BUCKET")
		if !set {
			return nil, errors.New("BUCKET environment variable not set")
		}

		var err error
		driver, err = gcs.New(ctx, bucket)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.Errorf("unkown driver '%s'", driverName)
	}
	return driver, nil
}
