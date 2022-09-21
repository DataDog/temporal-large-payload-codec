package v2

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/DataDog/temporal-large-payload-codec/logging"
	"hash"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/DataDog/temporal-large-payload-codec/server/storage"
)

// NewHandler creates a v2 HTTP handler for the Large Payload Service.
//
// Compared to v1, this version decouples the storage path from the digest/checksum.
// It also implements checksum validation.
func NewHandler(driver storage.Driver, logger logging.Logger) http.Handler {
	r := http.NewServeMux()
	handler := &blobHandler{
		driver,
		1024 * 1024 * 1024, // 1 GB
		logger,
	}

	r.HandleFunc("/v2/health/head", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			handler.handleError(w, nil, http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	r.HandleFunc("/v2/blobs/put", handler.putBlob)
	r.HandleFunc("/v2/blobs/get", handler.getBlob)

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

	keyParam := r.URL.Query().Get("key")
	if keyParam == "" {
		b.handleError(w, errors.New("key query parameter is required"), http.StatusBadRequest)
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

	if _, err := b.driver.GetPayload(r.Context(), &storage.GetRequest{Key: keyParam, Writer: w}); err != nil {
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

	digestParam := r.URL.Query().Get("digest")
	if digestParam == "" {
		b.handleError(w, errors.New("digest query parameter is required"), http.StatusBadRequest)
		return
	}

	digest, hash, err := b.digestAndHash(digestParam)
	if err != nil {
		b.handleError(w, err, http.StatusBadRequest)
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

	tee := io.TeeReader(r.Body, hash)

	result, err := b.driver.PutPayload(r.Context(), &storage.PutRequest{
		Metadata:      metadata,
		Data:          tee,
		Key:           b.computeKey(digestParam),
		Digest:        digestParam,
		ContentLength: contentLength,
	})
	if err != nil {
		b.handleError(w, err, http.StatusInternalServerError)
		return
	}

	checkSum := hex.EncodeToString(hash.Sum(nil))
	if checkSum != digest {
		b.handleError(w, errors.New("checksum mismatch"), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(result); err != nil {
		return
	}
}

func (b *blobHandler) digestAndHash(digest string) (string, hash.Hash, error) {
	tokens := strings.Split(digest, ":")
	if len(tokens) != 2 {
		return "", nil, fmt.Errorf("invalid digest format '%s'", digest)
	}

	var h hash.Hash
	switch tokens[0] {
	case "sha256":
		h = sha256.New()
	default:
		return "", nil, fmt.Errorf("invalid hash type '%s'", tokens[0])
	}
	return tokens[1], h, nil
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
