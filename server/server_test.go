// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package server

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/temporal-large-payload-codec/server/storage"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/memory"
)

func TestGetBlobV2(t *testing.T) {
	driver := &memory.Driver{}
	testPayloadBytes := []byte("hello world")
	putResponse, err := driver.PutPayload(context.Background(), &storage.PutRequest{
		Data:          bytes.NewReader(testPayloadBytes),
		Key:           "blobs/sha256:3b336ba10c19d14d5e741d7b76957bb88620a282d92aac23e2d81c2393f1451d",
		Digest:        "sha256:3b336ba10c19d14d5e741d7b76957bb88620a282d92aac23e2d81c2393f1451d",
		ContentLength: uint64(len(testPayloadBytes)),
	})
	require.NoError(t, err)

	testCase := []struct {
		name        string
		target      string
		method      string
		headers     map[string]string
		queryParams map[string]string
		want        string
		statusCode  int
	}{
		{
			name:   "No Content type specified",
			target: "blobs/get",
			method: http.MethodGet,
			headers: map[string]string{
				"X-Payload-Expected-Content-Length": "10",
			},
			queryParams: map[string]string{
				"digest": "12345",
			},
			want:       `missing or incorrect Content-Type header`,
			statusCode: http.StatusBadRequest,
		},
		{
			name:   "Wrong Content type specified",
			target: "blobs/get",
			method: http.MethodGet,
			headers: map[string]string{
				"Content-Type":                      "text/*",
				"X-Payload-Expected-Content-Length": "10",
			},
			queryParams: map[string]string{
				"digest": "12345",
			},
			want:       `missing or incorrect Content-Type header`,
			statusCode: http.StatusBadRequest,
		},
		{
			name:   "Missing key",
			target: "blobs/get",
			method: http.MethodGet,
			headers: map[string]string{
				"Content-Type":                      "application/octet-stream",
				"X-Payload-Expected-Content-Length": "10",
			},
			queryParams: map[string]string{},
			want:        `key query parameter is required`,
			statusCode:  http.StatusBadRequest,
		},
		{
			name:   "Missing length",
			target: "blobs/get",
			method: http.MethodGet,
			headers: map[string]string{
				"Content-Type": "application/octet-stream",
			},
			queryParams: map[string]string{
				"key": "sha256:12345",
			},
			want:       `expected content length header is required`,
			statusCode: http.StatusBadRequest,
		},
		{
			name:   "Successful retrieval",
			target: "blobs/get",
			method: http.MethodGet,
			headers: map[string]string{
				"Content-Type":                      "application/octet-stream",
				"X-Payload-Expected-Content-Length": "10",
			},
			queryParams: map[string]string{
				"key": putResponse.Key,
			},
			want:       `hello world`,
			statusCode: http.StatusOK,
		},
		{
			name:   "Successful retrieval while decoding key",
			target: "blobs/get",
			method: http.MethodGet,
			headers: map[string]string{
				"Content-Type":                      "application/octet-stream",
				"X-Payload-Expected-Content-Length": "10",
			},
			queryParams: map[string]string{
				"key": url.QueryEscape(putResponse.Key),
			},
			want:       `hello world`,
			statusCode: http.StatusOK,
		},
	}

	for _, scenario := range testCase {
		t.Run(scenario.name, func(t *testing.T) {
			request := httptest.NewRequest(scenario.method, fmt.Sprintf("/v2/%s", scenario.target), nil)
			// add headers
			for k, v := range scenario.headers {
				request.Header.Set(k, v)
			}

			// add query parameters
			q := request.URL.Query()
			for k, v := range scenario.queryParams {
				q.Add(k, v)
			}
			request.URL.RawQuery = q.Encode()

			responseRecorder := httptest.NewRecorder()

			handler := NewHttpHandler(driver)
			handler.ServeHTTP(responseRecorder, request)

			assert.Equal(t, scenario.statusCode, responseRecorder.Code)
			assert.Equal(t, scenario.want, responseRecorder.Body.String())
		})
	}
}
