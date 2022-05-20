package main

import (
	"log"
	"net/http"

	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/DataDog/temporal-large-payload-codec/internal/driver/s3"
	"github.com/DataDog/temporal-large-payload-codec/internal/server"
)

func main() {
	mux := http.NewServeMux()
	mux.Handle("/codec", server.NewHttpHandler(s3.New(&s3.Config{
		Session: session.Must(session.NewSession()),
		Bucket:  "todo",
	})))

	if err := http.ListenAndServe(":8233", mux); err != nil {
		log.Fatal(err)
	}
}
