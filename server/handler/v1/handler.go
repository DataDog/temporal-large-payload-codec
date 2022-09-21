package v1

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/DataDog/temporal-large-payload-codec/logging"
	"net/http"
	"strconv"

	"github.com/DataDog/temporal-large-payload-codec/server/storage"
)

// NewHandler creates a v1 HTTP handler for the Large Payload Service.
//
// Deprecated: This handler exists for backwards compatibility in order to
// read large payloads persisted with version v1.
// This handler will eventually be removed.
func NewHandler(driver storage.Driver, logger logging.Logger) http.Handler {
	r := http.NewServeMux()
	handler := &blobHandler{
		driver,
		1024 * 1024 * 1024, // 1 GB
		logger,
	}

	r.HandleFunc("/v1/health/head", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			handler.handleError(w, nil, http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	r.HandleFunc("/v1/blobs/put", handler.putBlob)
	r.HandleFunc("/v1/blobs/upload", handler.putBlob) // temporarily re-added for backwards compatibility
	r.HandleFunc("/v1/blobs/get", handler.getBlob)

	return r
}

type blobHandler struct {
	driver       storage.Driver
	maxBlobBytes uint64
	logger       logging.Logger
}

func (b *blobHandler) getBlob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		b.handleError(w, nil, http.StatusMethodNotAllowed)
		return
	}
	if contentType := r.Header.Get("Content-Type"); contentType != "application/octet-stream" {
		b.handleError(w, fmt.Errorf("missing or incorrect Content-Type header"), http.StatusBadRequest)
		return
	}
	digest := r.URL.Query().Get("digest")
	if digest == "" {
		b.handleError(w, fmt.Errorf("digest query parameter is required"), http.StatusBadRequest)
		return
	}

	expectedLengthHeader := r.Header.Get("X-Payload-Expected-Content-Length")
	if expectedLengthHeader == "" {
		b.handleError(w, fmt.Errorf("expected content length header is required"), http.StatusBadRequest)
		return
	}
	expectedLength, err := strconv.ParseUint(expectedLengthHeader, 10, 64)
	if err != nil {
		b.handleError(w, fmt.Errorf("expected content length header %s is invalid: %w", expectedLengthHeader, err), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Length", strconv.FormatUint(expectedLength, 10))

	if _, err := b.driver.GetPayload(r.Context(), &storage.GetRequest{Key: b.computeKey(digest), Writer: w}); err != nil {
		w.Header().Del("Content-Length") // unset Content-Length on errors

		var blobNotFound *storage.ErrBlobNotFound
		if errors.As(err, &blobNotFound) {
			b.handleError(w, err, http.StatusNotFound)
		} else {
			b.handleError(w, err, http.StatusInternalServerError)
		}
	}
}

func (b *blobHandler) putBlob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		b.handleError(w, nil, http.StatusMethodNotAllowed)
		return
	}
	if contentType := r.Header.Get("Content-Type"); contentType != "application/octet-stream" {
		b.handleError(w, fmt.Errorf("missing or incorrect Content-Type header"), http.StatusBadRequest)
		return
	}
	digest := r.URL.Query().Get("digest")
	if digest == "" {
		b.handleError(w, fmt.Errorf("digest query parameter is required"), http.StatusBadRequest)
		return
	}
	contentLengthHeader := r.Header.Get("Content-Length")
	if contentLengthHeader == "" {
		b.handleError(w, nil, http.StatusLengthRequired)
		return
	}
	contentLength, err := strconv.ParseUint(contentLengthHeader, 10, 64)
	if err != nil {
		b.handleError(w, err, http.StatusBadRequest)
		return
	}
	if contentLength > b.maxBlobBytes {
		b.handleError(w, fmt.Errorf("payload exceeds max size of %d bytes", b.maxBlobBytes), http.StatusRequestEntityTooLarge)
		return
	}

	rawMetadata, err := base64.StdEncoding.DecodeString(r.Header.Get("X-Temporal-Metadata"))
	if err != nil {
		b.handleError(w, err, http.StatusBadRequest)
		return
	}
	var metadata map[string][]byte
	if err := json.Unmarshal(rawMetadata, &metadata); err != nil {
		b.handleError(w, err, http.StatusBadRequest)
		return
	}

	result, err := b.driver.PutPayload(r.Context(), &storage.PutRequest{
		Metadata:      metadata,
		Data:          r.Body,
		Key:           b.computeKey(digest),
		Digest:        digest,
		ContentLength: contentLength,
	})
	if err != nil {
		b.handleError(w, err, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(result); err != nil {
		return
	}
}

func (b *blobHandler) handleError(w http.ResponseWriter, err error, statusCode int) {
	if err != nil {
		b.logger.Error(err.Error())
	}
	w.WriteHeader(statusCode)
	if err != nil {
		_, _ = w.Write([]byte(err.Error()))
	}
	return
}

func (b *blobHandler) computeKey(digest string) string {
	return fmt.Sprintf("blobs/%s", digest)
}
