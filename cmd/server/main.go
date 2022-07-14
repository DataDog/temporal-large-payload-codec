package main

import (
	"context"
	"log"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/config"

	"github.com/DataDog/temporal-large-payload-codec/server"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/s3"
)

func main() {
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion("YOUR_REGION_HERE"),
		config.WithSharedConfigProfile("YOUR_PROFILE_HERE"),
	)
	if err != nil {
		log.Fatal(err)
	}
	if err := http.ListenAndServe(":8577", server.NewHttpHandler(s3.New(&s3.Config{
		Config: cfg,
		Bucket: "YOUR_BUCKET_HERE",
	}))); err != nil {
		log.Fatal(err)
	}
}
