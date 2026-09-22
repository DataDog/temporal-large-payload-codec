// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package v2

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/DataDog/temporal-large-payload-codec/server/logging"
	"github.com/DataDog/temporal-large-payload-codec/server/storage"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type trackingReadCloser struct {
	reader io.Reader
	read   int
	eof    bool
}

func (r *trackingReadCloser) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.read += n
	if errors.Is(err, io.EOF) {
		r.eof = true
	}
	return n, err
}

func (r *trackingReadCloser) Close() error {
	return nil
}

type observingResponseWriter struct {
	*httptest.ResponseRecorder
	beforeWriteHeader func(int)
}

func (w *observingResponseWriter) WriteHeader(statusCode int) {
	if w.beforeWriteHeader != nil {
		w.beforeWriteHeader(statusCode)
	}
	w.ResponseRecorder.WriteHeader(statusCode)
}

type errorAfterReader struct {
	data []byte
	err  error
}

func (r *errorAfterReader) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	return 0, r.err
}

type readingDriver struct{}

func (d *readingDriver) PutPayload(_ context.Context, request *storage.PutRequest) (*storage.PutResponse, error) {
	if _, err := io.Copy(io.Discard, request.Data); err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}
	return &storage.PutResponse{Key: request.Key}, nil
}

func (d *readingDriver) GetPayload(context.Context, *storage.GetRequest) (*storage.GetResponse, error) {
	panic("unexpected GetPayload call")
}

func (d *readingDriver) ExistPayload(context.Context, *storage.ExistRequest) (*storage.ExistResponse, error) {
	return &storage.ExistResponse{Exists: false}, nil
}

func (d *readingDriver) DeletePayload(context.Context, *storage.DeleteRequest) (*storage.DeleteResponse, error) {
	panic("unexpected DeletePayload call")
}

func Test_putBlobDeduplicatedRequestDrainsBody(t *testing.T) {
	payload := bytes.Repeat([]byte("payload"), 256*1024)
	handler, digest, metadataHeader := newDeduplicatedPutHandler(t, payload, 1024*1024*1024)
	body := &trackingReadCloser{reader: bytes.NewReader(payload)}
	request := newPutRequest(body, digest, metadataHeader, len(payload))
	response := &observingResponseWriter{
		ResponseRecorder: httptest.NewRecorder(),
		beforeWriteHeader: func(statusCode int) {
			if statusCode == http.StatusOK {
				assert.Equal(t, len(payload), body.read)
				assert.True(t, body.eof)
			}
		},
	}

	handler.putBlob(response, request)

	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, len(payload), body.read)
	assert.True(t, body.eof)
}

func Test_putBlobDeduplicatedRequestBodyBoundaries(t *testing.T) {
	storedPayload := []byte("stored payload")

	t.Run("body exceeds declared length", func(t *testing.T) {
		handler, digest, metadataHeader := newDeduplicatedPutHandler(t, storedPayload, 16)
		body := &trackingReadCloser{reader: bytes.NewReader(bytes.Repeat([]byte("x"), 32))}
		response := httptest.NewRecorder()

		handler.putBlob(response, newPutRequest(body, digest, metadataHeader, 8))

		assert.Equal(t, http.StatusRequestEntityTooLarge, response.Code)
		assert.Equal(t, 9, body.read)
		assert.NotContains(t, response.Body.String(), "\"Key\"")
	})

	t.Run("clean short body", func(t *testing.T) {
		handler, digest, metadataHeader := newDeduplicatedPutHandler(t, storedPayload, 16)
		body := &trackingReadCloser{reader: bytes.NewReader([]byte("short"))}
		response := httptest.NewRecorder()

		handler.putBlob(response, newPutRequest(body, digest, metadataHeader, 8))

		assert.Equal(t, http.StatusBadRequest, response.Code)
		assert.True(t, body.eof)
		assert.NotContains(t, response.Body.String(), "\"Key\"")
	})

	t.Run("unexpected EOF", func(t *testing.T) {
		handler, digest, metadataHeader := newDeduplicatedPutHandler(t, storedPayload, 16)
		body := &trackingReadCloser{reader: &errorAfterReader{data: []byte("short"), err: io.ErrUnexpectedEOF}}
		response := httptest.NewRecorder()

		handler.putBlob(response, newPutRequest(body, digest, metadataHeader, 8))

		assert.Equal(t, http.StatusInternalServerError, response.Code)
		assert.NotContains(t, response.Body.String(), "\"Key\"")
	})

	t.Run("other reader error", func(t *testing.T) {
		handler, digest, metadataHeader := newDeduplicatedPutHandler(t, storedPayload, 16)
		body := &trackingReadCloser{reader: &errorAfterReader{data: []byte("short"), err: errors.New("read failed")}}
		response := httptest.NewRecorder()

		handler.putBlob(response, newPutRequest(body, digest, metadataHeader, 8))

		assert.Equal(t, http.StatusInternalServerError, response.Code)
		assert.NotContains(t, response.Body.String(), "\"Key\"")
	})

	t.Run("zero length", func(t *testing.T) {
		handler, digest, metadataHeader := newDeduplicatedPutHandler(t, nil, 16)
		body := &trackingReadCloser{reader: bytes.NewReader(nil)}
		response := httptest.NewRecorder()

		handler.putBlob(response, newPutRequest(body, digest, metadataHeader, 0))

		assert.Equal(t, http.StatusOK, response.Code)
		assert.Equal(t, 0, body.read)
		assert.True(t, body.eof)
	})
}

func Test_putBlobPhysicalWriteBodyBoundaries(t *testing.T) {
	t.Run("exact length", func(t *testing.T) {
		payload := []byte("payload")
		digestBytes := sha256.Sum256(payload)
		digest := "sha256:" + hex.EncodeToString(digestBytes[:])
		body := &trackingReadCloser{reader: bytes.NewReader(payload)}
		response := httptest.NewRecorder()
		handler := &blobHandler{driver: &readingDriver{}, maxBlobBytes: 16, logger: logging.NewNoopLogger()}

		handler.putBlob(response, newPutRequest(body, digest, encodedEmptyMetadata(t), len(payload)))

		assert.Equal(t, http.StatusCreated, response.Code)
	})

	t.Run("wrapped overrun error", func(t *testing.T) {
		payload := bytes.Repeat([]byte("x"), 16)
		digestBytes := sha256.Sum256(payload)
		digest := "sha256:" + hex.EncodeToString(digestBytes[:])
		body := &trackingReadCloser{reader: bytes.NewReader(payload)}
		response := httptest.NewRecorder()
		handler := &blobHandler{driver: &readingDriver{}, maxBlobBytes: 16, logger: logging.NewNoopLogger()}

		handler.putBlob(response, newPutRequest(body, digest, encodedEmptyMetadata(t), 8))

		assert.Equal(t, http.StatusRequestEntityTooLarge, response.Code)
		assert.Equal(t, 9, body.read)
	})
}

func Test_putBlobEarlyValidationDoesNotReadBody(t *testing.T) {
	server := httptest.NewServer(NewHandler(&memory.Driver{}, logging.NewNoopLogger()))
	t.Cleanup(server.Close)
	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	for _, expectContinue := range []bool{false, true} {
		name := "without Expect"
		if expectContinue {
			name = "with Expect"
		}
		t.Run(name, func(t *testing.T) {
			conn, err := net.DialTimeout("tcp", serverURL.Host, time.Second)
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })
			require.NoError(t, conn.SetDeadline(time.Now().Add(2*time.Second)))

			headers := fmt.Sprintf("PUT /v2/blobs/put?digest=sha256:test HTTP/1.1\r\nHost: %s\r\nContent-Type: application/octet-stream\r\nContent-Length: %d\r\n", serverURL.Host, 300*1024)
			if expectContinue {
				headers += "Expect: 100-continue\r\n"
			}
			_, err = io.WriteString(conn, headers+"\r\n")
			require.NoError(t, err)

			response, err := http.ReadResponse(bufio.NewReader(conn), nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = response.Body.Close() })
			assert.Equal(t, http.StatusBadRequest, response.StatusCode)
		})
	}
}

func newDeduplicatedPutHandler(t *testing.T, payload []byte, maxBlobBytes uint64) (*blobHandler, string, string) {
	t.Helper()
	digestBytes := sha256.Sum256(payload)
	digest := "sha256:" + hex.EncodeToString(digestBytes[:])
	metadata := map[string][]byte{}
	metadataHeader := encodedEmptyMetadata(t)

	handler := &blobHandler{maxBlobBytes: maxBlobBytes}
	key, err := handler.computeKey("test", digest, metadata)
	require.NoError(t, err)

	driver := &memory.Driver{}
	_, err = driver.PutPayload(t.Context(), &storage.PutRequest{
		Data:          bytes.NewReader(payload),
		Key:           key,
		Digest:        digest,
		ContentLength: uint64(len(payload)),
	})
	require.NoError(t, err)
	handler.driver = driver
	handler.logger = logging.NewNoopLogger()
	return handler, digest, metadataHeader
}

func encodedEmptyMetadata(t *testing.T) string {
	t.Helper()
	metadataJSON, err := json.Marshal(map[string][]byte{})
	require.NoError(t, err)
	return base64.StdEncoding.EncodeToString(metadataJSON)
}

func newPutRequest(body io.ReadCloser, digest string, metadataHeader string, contentLength int) *http.Request {
	request := httptest.NewRequest(http.MethodPut, "/v2/blobs/put?namespace=test&digest="+digest, body)
	request.Body = body
	request.ContentLength = int64(contentLength)
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Content-Length", strconv.Itoa(contentLength))
	request.Header.Set("X-Temporal-Metadata", metadataHeader)
	return request
}

func Test_computeKey(t *testing.T) {
	h := blobHandler{}

	testCase := []struct {
		name        string
		namespace   string
		digest      string
		meta        map[string][]byte
		expectedKey string
		expectError bool
	}{
		{
			name:        "no prefix",
			namespace:   "foo",
			digest:      "sha256:1234",
			meta:        map[string][]byte{},
			expectedKey: "/blobs/foo/common/sha256:1234/sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			expectError: false,
		},
		{
			name:        "valid prefix",
			namespace:   "foo",
			digest:      "sha256:1234",
			meta:        map[string][]byte{keyPrefixName: []byte("a/b/c")},
			expectedKey: "/blobs/foo/custom/a/b/c/sha256:1234/sha256:02b711154c4e88a46ff26dc96f492ce38c8c9fe00f3b6b2ea1ef6c209a2f3bd7",
			expectError: false,
		},
		{
			name:        "invalid prefix",
			namespace:   "foo",
			digest:      "sha256:1234",
			meta:        map[string][]byte{keyPrefixName: []byte("../../a")},
			expectedKey: "",
			expectError: true,
		},
		{
			name:        "invalid prefix ii",
			namespace:   "foo",
			digest:      "sha256:1234",
			meta:        map[string][]byte{keyPrefixName: []byte("a$(foo)b")},
			expectedKey: "",
			expectError: true,
		},
	}

	for _, scenario := range testCase {
		t.Run(scenario.name, func(t *testing.T) {
			key, err := h.computeKey(scenario.namespace, scenario.digest, scenario.meta)
			if scenario.expectError {
				assert.Error(t, err)
				assert.Empty(t, key)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, scenario.expectedKey, key)
			}
		})
	}
}
