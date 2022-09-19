package server

import (
	"github.com/DataDog/temporal-large-payload-codec/logging"
	v1 "github.com/DataDog/temporal-large-payload-codec/server/handler/v1"
	v2 "github.com/DataDog/temporal-large-payload-codec/server/handler/v2"
	"github.com/DataDog/temporal-large-payload-codec/server/storage"
	"net/http"
)

func NewHttpHandler(driver storage.Driver) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/v1/", v1.NewHandler(driver, logging.NewNoopLogger()))
	mux.Handle("/v2/", v2.NewHandler(driver, logging.NewNoopLogger()))
	return mux
}

func NewHttpHandlerWithLogger(driver storage.Driver, logger logging.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/v1/", v1.NewHandler(driver, logger))
	mux.Handle("/v2/", v2.NewHandler(driver, logger))
	return mux
}
