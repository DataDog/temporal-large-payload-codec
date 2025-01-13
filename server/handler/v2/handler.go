// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package v2

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/DataDog/temporal-large-payload-codec/server/logging"
	"github.com/DataDog/temporal-large-payload-codec/server/storage"
	"hash"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	keyPrefixName = "remote-codec/key-prefix"
)

var (
	validPrefix = regexp.MustCompile(`^[0-9a-zA-Z_\-/]+$`).MatchString
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

	keyParam := r.URL.Query().Get("key")
	if keyParam == "" {
		b.handleError(w, errors.New("key query parameter is required"), http.StatusBadRequest)
		return
	}
	key, err := url.QueryUnescape(keyParam)
	if err != nil {
		b.handleError(w, fmt.Errorf("key query parameter %s cannot be unescaped: %w", keyParam, err), http.StatusBadRequest)
	}

	if _, err := b.driver.GetPayload(r.Context(), &storage.GetRequest{Key: key, Writer: w}); err != nil {
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

	namespaceParam := r.URL.Query().Get("namespace")
	if namespaceParam == "" {
		b.handleError(w, errors.New("namespace query parameter is required"), http.StatusBadRequest)
		return
	}

	digestParam := r.URL.Query().Get("digest")
	if digestParam == "" {
		b.handleError(w, errors.New("digest query parameter is required"), http.StatusBadRequest)
		return
	}

	digest, hasher, err := b.digestAndHash(digestParam)
	if err != nil {
		b.handleError(w, err, http.StatusBadRequest)
		return
	}

	temporalMetadata, err := b.decodeTemporalMetadata(r)
	if err != nil {
		b.handleError(w, err, http.StatusBadRequest)
		return
	}

	key, err := b.computeKey(namespaceParam, digestParam, temporalMetadata)
	if err != nil {
		b.handleError(w, err, http.StatusBadRequest)
		return
	}

	existResponse, err := b.driver.ExistPayload(r.Context(), &storage.ExistRequest{Key: key})
	if err != nil {
		b.handleError(w, err, http.StatusInternalServerError)
		return
	}
	if existResponse.Exists {
		w.WriteHeader(http.StatusOK)
		r := storage.PutResponse{
			Key: key,
		}
		if err := json.NewEncoder(w).Encode(r); err != nil {
			b.handleError(w, err, http.StatusInternalServerError)
		}
		return
	}

	tee := io.TeeReader(r.Body, hasher)
	result, err := b.driver.PutPayload(r.Context(), &storage.PutRequest{
		Data:          tee,
		Key:           key,
		Digest:        digestParam,
		ContentLength: contentLength,
	})
	if err != nil {
		b.handleError(w, err, http.StatusInternalServerError)
		return
	}

	checkSum := hex.EncodeToString(hasher.Sum(nil))
	if checkSum != digest {
		b.handleError(w, errors.New("checksum mismatch"), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(result); err != nil {
		return
	}
}

func (b *blobHandler) decodeTemporalMetadata(r *http.Request) (map[string][]byte, error) {
	rawMetadata, err := base64.StdEncoding.DecodeString(r.Header.Get("X-Temporal-Metadata"))
	if err != nil {
		return nil, err
	}
	var metadata map[string][]byte
	if err := json.Unmarshal(rawMetadata, &metadata); err != nil {
		return nil, err
	}
	return metadata, nil
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

func (b *blobHandler) computeKey(namespace string, dataDigest string, metadata map[string][]byte) (string, error) {
	metadataHash := hashMetadata(metadata)
	var key string

	prefix := string(metadata[keyPrefixName])
	if prefix == "" {
		key = fmt.Sprintf("/blobs/%s/common/%s/%s", namespace, dataDigest, metadataHash)
	} else {
		if !validPrefix(prefix) {
			return "", fmt.Errorf("'%s' is not a valid prefix", prefix)
		}
		key = fmt.Sprintf("/blobs/%s/custom/%s/%s/%s", namespace, prefix, dataDigest, metadataHash)
	}
	return key, nil
}

func hashMetadata(metadata map[string][]byte) string {
	i := 0
	keys := make([]string, len(metadata))
	for k := range metadata {
		keys[i] = k
		i++
	}
	sort.Strings(keys)

	h := sha256.New()
	for _, k := range keys {
		v := metadata[k]
		h.Write([]byte(k))
		h.Write(v)
	}

	return fmt.Sprintf("sha256:%s", hex.EncodeToString(h.Sum(nil)))
}
