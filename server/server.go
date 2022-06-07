package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/DataDog/temporal-large-payload-codec/server/storage"
)

func NewHttpHandler(driver storage.Driver) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/v1/", newV1Handler(driver))
	return mux
}

func newV1Handler(driver storage.Driver) http.Handler {
	r := http.NewServeMux()
	handler := &blobHandler{
		driver,
		1024 * 1024 * 1024, // 1 GB
	}

	r.HandleFunc("/v1/health/head", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			handleError(w, nil, http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	r.HandleFunc("/v1/blobs/upload", handler.putBlob)
	r.HandleFunc("/v1/blobs/get", handler.getBlob)

	return r
}

type blobHandler struct {
	driver       storage.Driver
	maxBlobBytes uint64
}

func (b *blobHandler) getBlob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		handleError(w, nil, http.StatusMethodNotAllowed)
		return
	}
	if contentType := r.Header.Get("Content-Type"); contentType != "application/octet-stream" {
		handleError(w, fmt.Errorf("missing or incorrect Content-Type header"), http.StatusBadRequest)
		return
	}
	digest := r.URL.Query().Get("digest")
	if digest == "" {
		handleError(w, fmt.Errorf("digest query parameter is required"), http.StatusBadRequest)
		return
	}

	resp, err := b.driver.GetPayload(r.Context(), &storage.GetRequest{Digest: digest})
	if err != nil {
		handleError(w, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Length", fmt.Sprintf("%d", resp.ContentLength))
	if _, err := io.Copy(w, resp.Data); err != nil {
		panic(err)
	}
}

func (b *blobHandler) putBlob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		handleError(w, nil, http.StatusMethodNotAllowed)
		return
	}
	if contentType := r.Header.Get("Content-Type"); contentType != "application/octet-stream" {
		handleError(w, fmt.Errorf("missing or incorrect Content-Type header"), http.StatusBadRequest)
		return
	}
	digest := r.URL.Query().Get("digest")
	if digest == "" {
		handleError(w, fmt.Errorf("digest query parameter is required"), http.StatusBadRequest)
		return
	}
	contentLength := r.Header.Get("Content-Length")
	if contentLength == "" {
		handleError(w, nil, http.StatusLengthRequired)
		return
	}
	length, err := strconv.ParseUint(contentLength, 10, 64)
	if err != nil {
		handleError(w, err, http.StatusBadRequest)
		return
	}
	if length > b.maxBlobBytes {
		handleError(w, fmt.Errorf("payload exceeds max size of %d bytes", b.maxBlobBytes), http.StatusRequestEntityTooLarge)
		return
	}

	result, err := b.driver.PutPayload(r.Context(), &storage.PutRequest{
		Payload:       r.Body,
		Digest:        digest,
		ContentLength: length,
	})
	if err != nil {
		handleError(w, err, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Location", result.Location)
	if err := json.NewEncoder(w).Encode(result); err != nil {
		return
	}
}

func handleError(w http.ResponseWriter, err error, statusCode int) {
	w.WriteHeader(statusCode)
	if err != nil {
		_, _ = w.Write([]byte(err.Error()))
	}
	return
}
