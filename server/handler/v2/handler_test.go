// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package v2

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/DataDog/temporal-large-payload-codec/server/logging"
	"github.com/DataDog/temporal-large-payload-codec/server/storage"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type trackingReadCloser struct {
	reader io.Reader
	read   int
}

func (r *trackingReadCloser) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.read += n
	return n, err
}

func (r *trackingReadCloser) Close() error {
	return nil
}

func Test_putBlobDeduplicatedRequestDrainsBody(t *testing.T) {
	payload := bytes.Repeat([]byte("payload"), 256*1024)
	digestBytes := sha256.Sum256(payload)
	digest := "sha256:" + hex.EncodeToString(digestBytes[:])
	metadata := map[string][]byte{}
	metadataJSON, err := json.Marshal(metadata)
	require.NoError(t, err)
	metadataHeader := base64.StdEncoding.EncodeToString(metadataJSON)

	handler := blobHandler{}
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

	body := &trackingReadCloser{reader: bytes.NewReader(payload)}
	request := httptest.NewRequest(http.MethodPut, "/v2/blobs/put?namespace=test&digest="+digest, body)
	request.Body = body
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Content-Length", strconv.Itoa(len(payload)))
	request.Header.Set("X-Temporal-Metadata", metadataHeader)
	response := httptest.NewRecorder()

	NewHandler(driver, logging.NewNoopLogger()).ServeHTTP(response, request)

	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, len(payload), body.read)
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
