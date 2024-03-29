// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package server

import (
	"net/http"

	"github.com/DataDog/temporal-large-payload-codec/logging"
	v2 "github.com/DataDog/temporal-large-payload-codec/server/handler/v2"

	"github.com/DataDog/temporal-large-payload-codec/server/storage"
)

// NewHttpHandler creates the default HTTP handler for the Large Payload Service using a
// noop logger.
func NewHttpHandler(driver storage.Driver) http.Handler {
	return NewHttpHandlerWithLogger(driver, logging.NewNoopLogger())
}

// NewHttpHandlerWithLogger creates a HTTP handler for the Large Payload Service using the
// specified logger.
func NewHttpHandlerWithLogger(driver storage.Driver, logger logging.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/v2/", v2.NewHandler(driver, logger))
	return mux
}
