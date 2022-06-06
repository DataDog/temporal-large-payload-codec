package main

import (
	"log"
	"net/http"

	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/DataDog/temporal-large-payload-codec/server"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/s3"
)

func main() {
	if err := http.ListenAndServe(":8577", server.NewHttpHandler(s3.New(&s3.Config{
		Session: session.Must(session.NewSession()),
		Bucket:  "todo",
	}))); err != nil {
		log.Fatal(err)
	}
}
